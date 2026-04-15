import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import multer from "multer";
import {
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import { getOrCreateUserMediaKey } from "./mediaKeys.js";

/* ============================================================
   SMALL UTILS
============================================================ */

function nowMs() {
  return Date.now();
}

function elapsed(start) {
  return Date.now() - start;
}

function logStage(reqId, label, extra) {
  if (extra !== undefined) {
    console.log(`[endpoint-publish ${reqId}] ${label}`, extra);
  } else {
    console.log(`[endpoint-publish ${reqId}] ${label}`);
  }
}

function safeUnlink(filePath) {
  try {
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch {}
}

function safeRm(dirPath) {
  try {
    if (dirPath && fs.existsSync(dirPath)) {
      fs.rmSync(dirPath, { recursive: true, force: true });
    }
  } catch {}
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function formatDurationText(totalSeconds) {
  const secs = Math.max(0, Math.floor(Number(totalSeconds) || 0));
  const hours = Math.floor(secs / 3600);
  const minutes = Math.floor((secs % 3600) / 60);
  const seconds = secs % 60;

  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function parseCreationData(raw) {
  if (raw == null) return {};
  if (typeof raw === "object" && !Array.isArray(raw)) return raw;

  const text = String(raw).trim();
  if (!text) return {};

  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
    return {};
  } catch {
    return {};
  }
}

function parseTags(rawTags) {
  if (Array.isArray(rawTags)) {
    return Array.from(
      new Set(
        rawTags
          .map((t) => String(t || "").trim().toLowerCase())
          .filter(Boolean)
          .slice(0, 30)
      )
    );
  }

  const text = String(rawTags || "").trim();
  if (!text) return [];

  try {
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return parseTags(parsed);
  } catch {}

  return Array.from(
    new Set(
      text
        .split(",")
        .map((t) => String(t || "").trim().toLowerCase())
        .filter(Boolean)
        .slice(0, 30)
    )
  );
}

function contentTypeForExt(ext) {
  const e = String(ext || "").toLowerCase();
  if (e === ".m3u8") return "application/vnd.apple.mpegurl";
  if (e === ".ts") return "video/mp2t";
  if (e === ".mp4") return "video/mp4";
  if (e === ".m4s") return "video/iso.segment";
  if (e === ".jpg" || e === ".jpeg") return "image/jpeg";
  if (e === ".png") return "image/png";
  return "application/octet-stream";
}

function listFilesRecursive(dir) {
  const out = [];
  const stack = [dir];

  while (stack.length) {
    const cur = stack.pop();
    const entries = fs.readdirSync(cur, { withFileTypes: true });

    for (const ent of entries) {
      const p = path.join(cur, ent.name);
      if (ent.isDirectory()) stack.push(p);
      else if (ent.isFile()) out.push(p);
    }
  }

  return out;
}

/* ============================================================
   PROCESS HELPERS
============================================================ */

function runCmd(cmd, args, { cwd } = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { windowsHide: true, cwd });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) return resolve({ out, err });
      reject(new Error(err || `${cmd} exited with code ${code}`));
    });
  });
}

async function getVideoDimensions(videoPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "v:0",
      "-show_entries",
      "stream=width,height",
      "-of",
      "json",
      videoPath,
    ]);

    const parsed = JSON.parse(out);
    const stream = parsed?.streams?.[0];

    const width = Number(stream?.width || 0);
    const height = Number(stream?.height || 0);

    if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0) {
      return null;
    }

    return { width, height };
  } catch {
    return null;
  }
}

async function getVideoDurationSeconds(videoPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-show_entries",
      "format=duration",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      videoPath,
    ]);

    const dur = Number(String(out).trim());
    if (!Number.isFinite(dur) || dur <= 0) return null;
    return dur;
  } catch {
    return null;
  }
}

async function generateThumbnailAtSecond(videoPath, thumbPath, seconds) {
  await runCmd("ffmpeg", [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(seconds),
    "-i",
    videoPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=480:270:force_original_aspect_ratio=decrease,pad=480:270:(ow-iw)/2:(oh-ih)/2",
    "-q:v",
    "4",
    thumbPath,
  ]);
}

async function generateThumbnailWithDuration(videoPath, thumbPath, durationSeconds) {
  let candidates = [];

  if (durationSeconds && durationSeconds > 0) {
    const half = clamp(
      Math.floor(durationSeconds * 0.5),
      1,
      Math.max(1, Math.floor(durationSeconds - 1))
    );

    candidates = [half, 3, 1, 10, 30]
      .map((t) =>
        clamp(Number(t), 0, Math.max(0, Math.floor(durationSeconds - 0.25)))
      )
      .filter((t) => Number.isFinite(t) && t >= 0);

    candidates = Array.from(new Set(candidates));
  } else {
    candidates = [3, 1, 10, 30];
  }

  let lastErr = null;

  for (const t of candidates) {
    try {
      await generateThumbnailAtSecond(videoPath, thumbPath, t);
      return t;
    } catch (e) {
      lastErr = e;
      safeUnlink(thumbPath);
    }
  }

  throw lastErr || new Error("Thumbnail generation failed");
}

async function generateHlsVOD(inputPath, outDir) {
  fs.mkdirSync(outDir, { recursive: true });

  const dims = await getVideoDimensions(inputPath);
  const width = Number(dims?.width || 0);
  const height = Number(dims?.height || 0);

  const isPortrait = height > width;
  const isSquare = width > 0 && height > 0 && Math.abs(width - height) < 8;

  let vf;

  if (isPortrait) {
    vf =
      "scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2,format=yuv420p";
  } else if (isSquare) {
    vf =
      "scale=1080:1080:force_original_aspect_ratio=decrease,pad=1080:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p";
  } else {
    vf =
      "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p";
  }

  const args = [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-threads",
    "0",
    "-i",
    inputPath,
    "-vf",
    vf,
    "-c:v",
    "libx264",
    "-preset",
    "superfast",
    "-crf",
    "24",
    "-pix_fmt",
    "yuv420p",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ac",
    "2",
    "-f",
    "hls",
    "-hls_time",
    "10",
    "-hls_playlist_type",
    "vod",
    "-hls_flags",
    "independent_segments",
    "-hls_segment_type",
    "mpegts",
    "-hls_segment_filename",
    path.join(outDir, "seg_%03d.ts"),
    path.join(outDir, "master.m3u8"),
  ];

  await runCmd("ffmpeg", args);
}

/* ============================================================
   S3 HELPERS
============================================================ */

function makeAssetsS3Client() {
  const region =
    process.env.S3_ASSETS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;

  if (!region) {
    throw new Error("Missing env S3_ASSETS_REGION (or AWS_REGION)");
  }

  const endpoint = process.env.S3_ASSETS_ENDPOINT || undefined;

  return new S3Client({
    region,
    ...(endpoint ? { endpoint } : {}),
  });
}

function makeUploadsS3Client() {
  const region =
    process.env.S3_UPLOADS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;

  if (!region) {
    throw new Error("Missing env S3_UPLOADS_REGION (or AWS_REGION)");
  }

  const endpoint = process.env.S3_UPLOADS_ENDPOINT || undefined;

  return new S3Client({
    region,
    ...(endpoint ? { endpoint } : {}),
  });
}

async function uploadFileToAssetsBucket({ assetsS3, bucket, key, filePath, contentType }) {
  const body = fs.createReadStream(filePath);

  await assetsS3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
      CacheControl: "public, max-age=31536000, immutable",
    })
  );
}

async function uploadLocalFileToS3({ s3, bucket, key, filePath, contentType }) {
  const body = fs.createReadStream(filePath);

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType || "application/octet-stream",
    })
  );
}

async function downloadS3ObjectToFile({ s3, bucket, key, filePath }) {
  const result = await s3.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );

  await new Promise((resolve, reject) => {
    const out = fs.createWriteStream(filePath);

    result.Body.on("error", reject);
    out.on("error", reject);
    out.on("close", resolve);

    result.Body.pipe(out);
  });
}

async function deleteS3ObjectIfExists({ s3, bucket, key }) {
  try {
    await s3.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
      })
    );
  } catch {}
}

async function retry(fn, { attempts = 2, delayMs = 500 } = {}) {
  let lastErr;

  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;

      const retryable =
        err?.message?.includes("socket hang up") ||
        err?.code === "ECONNRESET" ||
        err?.code === "ETIMEDOUT";

      if (!retryable || i === attempts - 1) throw err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }

  throw lastErr;
}

async function uploadDirToS3Concurrent({
  uploadFileToS3,
  bucket,
  localDir,
  keyPrefix,
  concurrency = 8,
}) {
  const files = listFilesRecursive(localDir);
  let index = 0;

  async function worker() {
    while (true) {
      const current = index++;
      if (current >= files.length) return;

      const filePath = files[current];
      const rel = path.relative(localDir, filePath).split(path.sep).join("/");
      const key = `${keyPrefix.replace(/\/+$/g, "")}/${rel}`;
      const ext = path.extname(filePath);

      await retry(() =>
        uploadFileToS3({
          bucket,
          key,
          filePath,
          contentType: contentTypeForExt(ext),
        })
      );
    }
  }

  const workerCount = Math.min(concurrency, Math.max(files.length, 1));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
}

/* ============================================================
   MODERATION HELPERS
============================================================ */

function extractSourceUrlsFromEditState(root) {
  const urls = new Set();

  const editState =
    root?.dna?.edit_state ||
    root?.edit_state ||
    root?.creation_data?.dna?.edit_state ||
    root?.creation_data?.edit_state ||
    null;

  if (!editState) return [];

  const trackGroups = [
    ...(Array.isArray(editState.videoTracks) ? editState.videoTracks : []),
    ...(Array.isArray(editState.audioTracks) ? editState.audioTracks : []),
  ];

  for (const track of trackGroups) {
    const clips = Array.isArray(track?.clips) ? track.clips : [];
    for (const clip of clips) {
      const sourceUrl = String(clip?.sourceUrl || "").trim();
      if (sourceUrl) urls.add(sourceUrl);
    }
  }

  return Array.from(urls);
}

function normalizePrimaryRenderMetadata(raw) {
  if (!raw || typeof raw !== "object") return null;

  return {
    id: raw.id ?? null,
    name: raw.name ?? null,
    type: raw.type ?? null,
    image: raw.image ?? null,
    timestamp: raw.timestamp ?? null,
    encryption_envelope: raw.encryption_envelope ?? raw.encryptionEnvelope ?? null,
    source_urls: extractSourceUrlsFromEditState(raw),
  };
}

function normalizeSourceMediaJson(raw) {
  if (!raw || typeof raw !== "object") return null;

  return {
    id: raw.id ?? null,
    name: raw.name ?? null,
    type: raw.type ?? null,
    image: raw.image ?? null,
    timestamp: raw.timestamp ?? null,
    source_urls: extractSourceUrlsFromEditState(raw),
    raw,
  };
}

function buildModerationPayload({ primaryRenderJson, sourceMediaJsons }) {
  const normalizedPrimary = normalizePrimaryRenderMetadata(primaryRenderJson);

  const normalizedSources = (Array.isArray(sourceMediaJsons) ? sourceMediaJsons : [])
    .map(normalizeSourceMediaJson)
    .filter(Boolean);

  const discoveredSourceUrls = new Set();

  for (const url of normalizedPrimary?.source_urls || []) {
    discoveredSourceUrls.add(url);
  }

  for (const item of normalizedSources) {
    for (const url of item.source_urls || []) {
      discoveredSourceUrls.add(url);
    }
    if (item.image) discoveredSourceUrls.add(item.image);
  }

  const required =
    normalizedSources.length > 0 ||
    (normalizedPrimary?.source_urls?.length || 0) > 0;

  return {
    required,
    status: required ? "pending" : "not_required",
    submitted_at: new Date().toISOString(),
    primary_render: normalizedPrimary,
    source_media: normalizedSources,
    all_source_urls: Array.from(discoveredSourceUrls),
  };
}

/* ============================================================
   CRYPTO
============================================================ */

function deriveBossStyleMasterKek(rawKey) {
  const bytes = Buffer.from(String(rawKey || ""), "utf8");
  if (bytes.length >= 32) return bytes.subarray(0, 32);
  return Buffer.concat([bytes, Buffer.alloc(32 - bytes.length)]);
}

async function unwrapDekBossStyle({ envelope, masterKek }) {
  const encDek = envelope?.encrypted_dek;
  const payload = Buffer.from(String(encDek || ""), "base64");

  if (payload.length < 29) {
    throw new Error("Invalid encrypted_dek payload");
  }

  const gcmIv = payload.subarray(0, 12);
  const tag = payload.subarray(12, 28);
  const encryptedDek = payload.subarray(28);

  const decipher = crypto.createDecipheriv("aes-256-gcm", masterKek, gcmIv);
  decipher.setAuthTag(tag);

  return Buffer.concat([decipher.update(encryptedDek), decipher.final()]);
}

function decryptFileAesCtr({ encryptedPath, outputPath, dekBuffer, ivB64 }) {
  const iv = Buffer.from(String(ivB64 || ""), "base64");

  if (!Buffer.isBuffer(dekBuffer) || dekBuffer.length !== 32) {
    throw new Error("DEK must be 32 bytes for AES-256-CTR");
  }

  if (iv.length !== 16) {
    throw new Error("media_iv must be 16 bytes (base64 encoded)");
  }

  return new Promise((resolve, reject) => {
    const decipher = crypto.createDecipheriv("aes-256-ctr", dekBuffer, iv);
    const input = fs.createReadStream(encryptedPath);
    const output = fs.createWriteStream(outputPath);

    let settled = false;

    const fail = (err) => {
      if (settled) return;
      settled = true;
      input.destroy();
      decipher.destroy();
      output.destroy();
      safeUnlink(outputPath);
      reject(err);
    };

    const succeed = () => {
      if (settled) return;
      settled = true;
      resolve();
    };

    input.on("error", fail);
    output.on("error", fail);
    decipher.on("error", fail);
    output.on("close", succeed);

    input.pipe(decipher).pipe(output);
  });
}

async function decryptFileWithEnvelopeSupport({
  encryptedPath,
  outputPath,
  rawMediaKey,
  ivB64,
  encryptionEnvelope,
}) {
  if (encryptionEnvelope) {
    const masterKek = deriveBossStyleMasterKek(rawMediaKey);

    const dek = await unwrapDekBossStyle({
      envelope: encryptionEnvelope,
      masterKek,
    });

    if (!Buffer.isBuffer(dek) || dek.length !== 32) {
      throw new Error(`Unwrapped DEK length was ${dek?.length ?? "unknown"}, expected 32`);
    }

    await decryptFileAesCtr({
      encryptedPath,
      outputPath,
      dekBuffer: dek,
      ivB64: encryptionEnvelope.media_iv,
    });

    return { mode: "envelope-aes-ctr" };
  }

  const keyBuffer = deriveBossStyleMasterKek(rawMediaKey);

  await decryptFileAesCtr({
    encryptedPath,
    outputPath,
    dekBuffer: keyBuffer,
    ivB64,
  });

  return { mode: "direct-aes-ctr" };
}

/* ============================================================
   DB HELPERS
============================================================ */

export async function updateJob(pool, jobId, patch = {}) {
  const sets = [];
  const values = [];
  let i = 1;

  for (const [k, v] of Object.entries(patch)) {
    sets.push(`${k} = $${i++}`);
    values.push(v);
  }

  if (!sets.length) return;

  values.push(jobId);

  await pool.query(
    `UPDATE publish_jobs SET ${sets.join(", ")} WHERE id = $${i}`,
    values
  );
}

export async function fetchJob(pool, jobId) {
  const result = await pool.query(
    `
    SELECT *
    FROM publish_jobs
    WHERE id = $1
    LIMIT 1
    `,
    [jobId]
  );

  return result.rows[0] || null;
}

export async function requeueStalePublishJobs(pool) {
  const result = await pool.query(`
    UPDATE publish_jobs
    SET
      status = 'queued',
      progress_stage = 'queued',
      progress_pct = 0,
      error_message = NULL,
      started_at = NULL,
      finished_at = NULL
    WHERE status = 'processing'
    RETURNING id
  `);

  return result.rows.map((row) => Number(row.id));
}

async function fetchVideoById(pool, videoId) {
  const result = await pool.query(
    `
    SELECT *
    FROM videos
    WHERE id = $1
    LIMIT 1
    `,
    [videoId]
  );

  return result.rows[0] || null;
}

/* ============================================================
   WORKER JOB PROCESSOR
============================================================ */

export async function processPublishJob({ pool, uploadFileToS3, job }) {
  const reqId = `job-${job.id}-${crypto.randomBytes(4).toString("hex")}`;
  const startedAt = nowMs();

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-endpoint-job-"));
  const outDir = path.join(tmpRoot, "out");
  const hlsDir = path.join(outDir, "hls");
  const mp4Dir = path.join(tmpRoot, "mp4");

  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(hlsDir, { recursive: true });
  fs.mkdirSync(mp4Dir, { recursive: true });

  const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
  const uploadsS3 = makeUploadsS3Client();

  let localSourceMp4Path = null;

  try {
    logStage(reqId, "job start", { jobId: job.id, videoId: job.video_id });

    await updateJob(pool, job.id, {
      error_message: null,
      finished_at: null,
    });

    if (!job.video_id) {
      throw new Error("Publish job missing video_id");
    }

    const video = await fetchVideoById(pool, job.video_id);
    if (!video) {
      throw new Error(`Video ${job.video_id} not found for publish job`);
    }

    if (!video.source_mp4_key) {
      throw new Error(`Video ${job.video_id} missing source_mp4_key`);
    }

    await pool.query(
      `
      UPDATE videos
      SET transcode_status = 'processing'
      WHERE id = $1
      `,
      [job.video_id]
    );

    const creationData = job.creation_data || {};
    const primaryRenderJson = creationData;
    const sourceMediaJsons = [];

    const moderation = buildModerationPayload({
      primaryRenderJson,
      sourceMediaJsons,
    });

    const finalCreationData = {
      ...creationData,
      moderation,
    };

    const originalExt =
      path.extname(video.source_mp4_key || "").toLowerCase() ||
      path.extname(job.original_filename || "").toLowerCase() ||
      ".mp4";

    localSourceMp4Path = path.join(
      mp4Dir,
      `source-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${originalExt || ".mp4"}`
    );

    await updateJob(pool, job.id, {
      progress_stage: "downloading_source_mp4",
      progress_pct: 10,
    });

    await downloadS3ObjectToFile({
      s3: uploadsS3,
      bucket: uploadsBucket,
      key: video.source_mp4_key,
      filePath: localSourceMp4Path,
    });

    logStage(reqId, "source mp4 downloaded", { source_mp4_key: video.source_mp4_key });

    await updateJob(pool, job.id, {
      progress_stage: "transcoding",
      progress_pct: 45,
    });

    const tHls = nowMs();
    const hlsBase = `endpoint-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`;

    await generateHlsVOD(localSourceMp4Path, hlsDir);

    logStage(reqId, "hls generated", { ms: elapsed(tHls) });

    safeUnlink(localSourceMp4Path);
    localSourceMp4Path = null;

    const localMaster = path.join(hlsDir, "master.m3u8");
    if (!fs.existsSync(localMaster)) {
      throw new Error("HLS export failed: master.m3u8 not created");
    }

    await updateJob(pool, job.id, {
      progress_stage: "uploading_hls",
      progress_pct: 75,
    });

    const hlsKeyPrefix = `hls/${job.user_id}/${hlsBase}`;
    const hlsMasterKey = `${hlsKeyPrefix}/master.m3u8`;

    const tUpload = nowMs();
    await uploadDirToS3Concurrent({
      uploadFileToS3,
      bucket: uploadsBucket,
      localDir: hlsDir,
      keyPrefix: hlsKeyPrefix,
      concurrency: 8,
    });

    logStage(reqId, "hls uploaded", { ms: elapsed(tUpload), hlsMasterKey });

    safeRm(hlsDir);

    await updateJob(pool, job.id, {
      progress_stage: "finalizing",
      progress_pct: 90,
    });

    await pool.query(
      `
      UPDATE videos
      SET
        filename = $2,
        hls_master_key = $2,
        playback_mode = 'hls',
        transcode_status = 'ready',
        creation_data = $3::jsonb
      WHERE id = $1
      `,
      [
        job.video_id,
        hlsMasterKey,
        JSON.stringify(finalCreationData || {}),
      ]
    );

    await updateJob(pool, job.id, {
      status: "complete",
      progress_stage: "complete",
      progress_pct: 100,
      finished_at: new Date(),
      video_id: job.video_id,
      error_message: null,
    });

    if (job.upload_path) {
      await deleteS3ObjectIfExists({
        s3: uploadsS3,
        bucket: uploadsBucket,
        key: job.upload_path,
      });
    }

    safeRm(tmpRoot);

    logStage(reqId, "job done", {
      totalMs: elapsed(startedAt),
      videoId: job.video_id,
      hlsMasterKey,
    });
  } catch (e) {
    console.error(`[endpoint-publish worker ${job.id}] error:`, e);

    await updateJob(pool, job.id, {
      status: "failed",
      progress_stage: "failed",
      finished_at: new Date(),
      error_message: e?.message || "Publish failed",
    }).catch(() => {});

    if (job.video_id) {
      await pool.query(
        `
        UPDATE videos
        SET transcode_status = 'failed'
        WHERE id = $1
        `,
        [job.video_id]
      ).catch(() => {});
    }

    safeUnlink(localSourceMp4Path);
    safeRm(tmpRoot);
  }
}

/* ============================================================
   MAIN ROUTES
============================================================ */

export function registerEndpointPublish(app, deps = {}) {
  const { pool, requireAuth } = deps;

  if (!pool) throw new Error("registerEndpointPublish: missing pool");
  if (!requireAuth) throw new Error("registerEndpointPublish: missing requireAuth");

  const TMP_UPLOAD_DIR = path.join(os.tmpdir(), "mytube-endpoint-publish-uploads");
  fs.mkdirSync(TMP_UPLOAD_DIR, { recursive: true });

  const upload = multer({
    storage: multer.diskStorage({
      destination: (_req, _file, cb) => cb(null, TMP_UPLOAD_DIR),
      filename: (_req, file, cb) => {
        const ext = path.extname(file.originalname || "").toLowerCase() || ".bin";
        cb(null, `${Date.now()}-${crypto.randomBytes(8).toString("hex")}${ext}`);
      },
    }),
    limits: {
      fileSize: 1024 * 1024 * 1024,
      files: 1,
    },
  });

  app.post(
    "/api/endpoint/publish",
    requireAuth,
    upload.single("media"),
    async (req, res) => {
      const reqId = `${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
      const userId = Number(req.user.id);
      const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
      const assetsBucket = process.env.S3_ASSETS_BUCKET;
      const uploadsS3 = makeUploadsS3Client();
      const assetsS3 = makeAssetsS3Client();

      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-endpoint-request-"));
      const decryptedDir = path.join(tmpRoot, "decrypted");
      const thumbsDir = path.join(tmpRoot, "thumbs");

      fs.mkdirSync(decryptedDir, { recursive: true });
      fs.mkdirSync(thumbsDir, { recursive: true });

      let decryptedPath = null;
      let thumbPath = null;

      try {
        logStage(reqId, "request start");

        const title = String(req.body?.title || "").trim();
        const description = String(req.body?.description || "").trim();
        const visibility = String(req.body?.visibility || "public")
          .trim()
          .toLowerCase();
        const tags = parseTags(req.body?.tags);
        const creationData = parseCreationData(req.body?.creationData);

        const encryptionEnvelope =
          creationData?.encryption_envelope ||
          creationData?.encryptionEnvelope ||
          null;

        if (!title) {
          safeUnlink(req.file?.path);
          safeRm(tmpRoot);
          return res.status(400).json({ error: "Title required" });
        }

        if (!req.file?.path) {
          safeRm(tmpRoot);
          return res.status(400).json({ error: "Encrypted media file required" });
        }

        const allowedVis = new Set(["public", "private", "unlisted"]);
        if (!allowedVis.has(visibility)) {
          safeUnlink(req.file?.path);
          safeRm(tmpRoot);
          return res.status(400).json({
            error: "Visibility must be public, private, or unlisted",
          });
        }

        if (!uploadsBucket) {
          safeUnlink(req.file?.path);
          safeRm(tmpRoot);
          return res.status(500).json({ error: "Missing env S3_UPLOADS_BUCKET" });
        }

        if (!assetsBucket) {
          safeUnlink(req.file?.path);
          safeRm(tmpRoot);
          return res.status(500).json({ error: "Missing env S3_ASSETS_BUCKET" });
        }

        if (!encryptionEnvelope?.encrypted_dek || !encryptionEnvelope?.media_iv) {
          safeUnlink(req.file?.path);
          safeRm(tmpRoot);
          return res.status(400).json({
            error: "creationData.encryption_envelope with encrypted_dek and media_iv is required",
          });
        }

        const originalFilename =
          String(req.body?.originalFilename || "").trim() ||
          String(req.file.originalname || "").trim() ||
          path.basename(req.file.path);

        const sourceExt =
          path.extname(originalFilename).toLowerCase() ||
          path.extname(req.file.path).toLowerCase() ||
          ".bin";

        const tempSourceKey = `tmp/endpoint-publish/${userId}/${Date.now()}-${crypto
          .randomBytes(8)
          .toString("hex")}${sourceExt}`;

        logStage(reqId, "uploading encrypted temp source", {
          bucket: uploadsBucket,
          key: tempSourceKey,
        });

        await retry(() =>
          uploadLocalFileToS3({
            s3: uploadsS3,
            bucket: uploadsBucket,
            key: tempSourceKey,
            filePath: req.file.path,
            contentType: "application/octet-stream",
          })
        );

        decryptedPath = path.join(
          decryptedDir,
          `decrypted-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.mp4`
        );

        const rawMediaKey = await getOrCreateUserMediaKey(userId);

        await decryptFileWithEnvelopeSupport({
          encryptedPath: req.file.path,
          outputPath: decryptedPath,
          rawMediaKey,
          ivB64: "",
          encryptionEnvelope,
        });

        safeUnlink(req.file.path);

        const durationSecondsRaw = await getVideoDurationSeconds(decryptedPath);
        const durationSeconds = Math.max(0, Math.floor(Number(durationSecondsRaw) || 0));
        const durationText = formatDurationText(durationSeconds);

        const thumbName = `thumb-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.jpg`;
        thumbPath = path.join(thumbsDir, thumbName);

        await generateThumbnailWithDuration(decryptedPath, thumbPath, durationSeconds);

        const sourceMp4Key = `source/${userId}/${Date.now()}-${crypto
          .randomBytes(8)
          .toString("hex")}.mp4`;

        await retry(() =>
          uploadLocalFileToS3({
            s3: uploadsS3,
            bucket: uploadsBucket,
            key: sourceMp4Key,
            filePath: decryptedPath,
            contentType: "video/mp4",
          })
        );

        const thumbKey = `thumbs/${userId}/${thumbName}`;

        await retry(() =>
          uploadFileToAssetsBucket({
            assetsS3,
            bucket: assetsBucket,
            key: thumbKey,
            filePath: thumbPath,
            contentType: "image/jpeg",
          })
        );

        const client = await pool.connect();
        let videoId;
        let jobId;

        try {
          await client.query("BEGIN");

          const videoInsert = await client.query(
            `
            INSERT INTO videos (
              user_id,
              title,
              description,
              category,
              visibility,
              media_type,
              asset_scope,
              filename,
              source_mp4_key,
              hls_master_key,
              playback_mode,
              transcode_status,
              thumb,
              duration_text,
              duration,
              views,
              tags,
              creation_data,
              moderation_check
            )
            VALUES (
              $1, $2, $3, 'Other', $4, 'video', 'public',
              $5, $5, NULL, 'mp4', 'queued',
              $6, $7, $8, 0, $9, $10::jsonb, false
            )
            RETURNING id
            `,
            [
              userId,
              title,
              description,
              visibility,
              sourceMp4Key,
              thumbKey,
              durationText,
              durationSeconds,
              tags,
              JSON.stringify(creationData || {}),
            ]
          );

          videoId = Number(videoInsert.rows[0].id);

          const jobInsert = await client.query(
            `
            INSERT INTO publish_jobs (
              user_id,
              video_id,
              title,
              description,
              visibility,
              tags,
              original_filename,
              upload_path,
              creation_data,
              status,
              progress_stage,
              progress_pct
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, 'queued', 'queued', 0)
            RETURNING id
            `,
            [
              userId,
              videoId,
              title,
              description,
              visibility,
              tags,
              originalFilename,
              tempSourceKey,
              JSON.stringify(creationData || {}),
            ]
          );

          jobId = Number(jobInsert.rows[0].id);

          await client.query("COMMIT");
        } catch (dbErr) {
          await client.query("ROLLBACK").catch(() => {});
          throw dbErr;
        } finally {
          client.release();
        }

        safeUnlink(decryptedPath);
        decryptedPath = null;
        safeUnlink(thumbPath);
        thumbPath = null;
        safeRm(tmpRoot);

        logStage(reqId, "immediate publish complete", {
          videoId,
          jobId,
          sourceMp4Key,
          thumbKey,
        });

        return res.status(202).json({
          ok: true,
          queued: true,
          jobId,
          videoId,
          status: "queued",
          playbackMode: "mp4",
          transcodeStatus: "queued",
          watchUrl: `/watch/${videoId}`,
        });
      } catch (e) {
        console.error(`[endpoint-publish ${reqId}] error:`, e);
        safeUnlink(req.file?.path);
        safeUnlink(decryptedPath);
        safeUnlink(thumbPath);
        safeRm(tmpRoot);

        return res.status(500).json({
          ok: false,
          error: e?.message || "Endpoint publish failed",
        });
      }
    }
  );

  app.get(
    "/api/endpoint/publish/:jobId",
    requireAuth,
    async (req, res) => {
      const jobId = Number(req.params.jobId);

      if (!Number.isFinite(jobId)) {
        return res.status(400).json({ error: "Invalid job id" });
      }

      try {
        const job = await fetchJob(pool, jobId);

        if (!job || Number(job.user_id) !== Number(req.user.id)) {
          return res.status(404).json({ error: "Job not found" });
        }

        let video = null;
        if (job.video_id) {
          video = await fetchVideoById(pool, job.video_id);
        }

        return res.json({
          ok: true,
          jobId: Number(job.id),
          status: job.status,
          progressStage: job.progress_stage,
          progressPct: Number(job.progress_pct || 0),
          error: job.error_message || null,
          videoId: job.video_id || null,
          watchUrl: job.video_id ? `/watch/${job.video_id}` : null,
          createdAt: job.created_at,
          startedAt: job.started_at,
          finishedAt: job.finished_at,
          playbackMode: video?.playback_mode || null,
          transcodeStatus: video?.transcode_status || null,
          sourceMp4Key: video?.source_mp4_key || null,
          hlsMasterKey: video?.hls_master_key || null,
        });
      } catch (e) {
        console.error("GET /api/endpoint/publish/:jobId error:", e);
        return res.status(500).json({ error: "Failed to load publish job" });
      }
    }
  );
}