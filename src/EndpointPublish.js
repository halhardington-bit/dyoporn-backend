import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import multer from "multer";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

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

function parseJsonArray(raw) {
  if (raw == null) return [];
  if (Array.isArray(raw)) return raw;

  const text = String(raw).trim();
  if (!text) return [];

  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
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
    "scale=640:-2",
    "-q:v",
    "3",
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
    "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
    "-c:v",
    "libx264",
    "-preset",
    "superfast",
    "-crf",
    "23",
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

async function uploadFileToAssetsBucket({ assetsS3, bucket, key, filePath, contentType }) {
  const Body = fs.createReadStream(filePath);

  await assetsS3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body,
      ContentType: contentType,
      CacheControl: "public, max-age=31536000, immutable",
    })
  );
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
  concurrency = 6,
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

  const workers = Array.from(
    { length: Math.min(concurrency, Math.max(files.length, 1)) },
    () => worker()
  );

  await Promise.all(workers);
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
   MAIN ROUTE
============================================================ */

export function registerEndpointPublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  if (!pool) throw new Error("registerEndpointPublish: missing pool");
  if (!requireAuth) throw new Error("registerEndpointPublish: missing requireAuth");
  if (!uploadFileToS3) throw new Error("registerEndpointPublish: missing uploadFileToS3");

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
      const startedAt = nowMs();
      const reqId = `${Date.now()}-${crypto.randomBytes(4).toString("hex")}`;
      const userId = Number(req.user.id);

      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-endpoint-publish-"));
      const decryptedDir = path.join(tmpRoot, "decrypted");
      const outDir = path.join(tmpRoot, "out");
      const hlsDir = path.join(outDir, "hls");

      fs.mkdirSync(decryptedDir, { recursive: true });
      fs.mkdirSync(outDir, { recursive: true });
      fs.mkdirSync(hlsDir, { recursive: true });

      const cleanup = () => {
        safeRm(tmpRoot);
        safeUnlink(req.file?.path);
      };

      try {
        logStage(reqId, "start");

        const title = String(req.body?.title || "").trim();
        const description = String(req.body?.description || "").trim();
        const visibility = String(req.body?.visibility || "public").trim().toLowerCase();
        const tags = parseTags(req.body?.tags);

        const iv = String(
          req.body?.iv ||
          req.body?.mediaIv ||
          req.body?.media_iv ||
          ""
        ).trim();

        const baseCreationData = parseCreationData(req.body?.creationData);
        const primaryRenderJson = baseCreationData;

        const sourceMediaJsons = [
          ...parseJsonArray(req.body?.sourceMediaJsons),
          ...parseJsonArray(req.body?.source_media_jsons),
          ...parseJsonArray(req.body?.moderationMediaJsons),
          ...parseJsonArray(req.body?.moderation_media_jsons),
        ];

        const encryptionEnvelope =
          primaryRenderJson?.encryption_envelope ||
          primaryRenderJson?.encryptionEnvelope ||
          null;

        if (!title) {
          cleanup();
          return res.status(400).json({ error: "Title required" });
        }

        if (!req.file?.path) {
          cleanup();
          return res.status(400).json({ error: "Encrypted media file required" });
        }

        const allowedVis = new Set(["public", "private", "unlisted"]);
        if (!allowedVis.has(visibility)) {
          cleanup();
          return res.status(400).json({
            error: "Visibility must be public, private, or unlisted",
          });
        }

        if (!process.env.S3_UPLOADS_BUCKET) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_UPLOADS_BUCKET" });
        }

        if (!process.env.S3_ASSETS_BUCKET) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_ASSETS_BUCKET" });
        }

        if (!encryptionEnvelope) {
          cleanup();
          return res.status(400).json({
            error: "creationData.encryption_envelope is required",
          });
        }

        if (!encryptionEnvelope.encrypted_dek) {
          cleanup();
          return res.status(400).json({
            error: "creationData.encryption_envelope.encrypted_dek required",
          });
        }

        if (!encryptionEnvelope.media_iv) {
          cleanup();
          return res.status(400).json({
            error: "creationData.encryption_envelope.media_iv required",
          });
        }

        const originalExt =
          path.extname(req.body?.originalFilename || "").toLowerCase() ||
          path.extname(req.file.originalname || "").toLowerCase() ||
          ".mp4";

        const decryptedPath = path.join(
          decryptedDir,
          `decrypted-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${originalExt || ".mp4"}`
        );

        const tKey = nowMs();
        const rawMediaKey = await getOrCreateUserMediaKey(userId);
        logStage(reqId, "media key loaded", { ms: elapsed(tKey) });

        const tDecrypt = nowMs();
        const decryptResult = await decryptFileWithEnvelopeSupport({
          encryptedPath: req.file.path,
          outputPath: decryptedPath,
          rawMediaKey,
          ivB64: iv,
          encryptionEnvelope,
        });
        logStage(reqId, "decrypted", { ms: elapsed(tDecrypt), mode: decryptResult.mode });

        const moderation = buildModerationPayload({
          primaryRenderJson,
          sourceMediaJsons,
        });

        const moderationCheck = false;

        const creationData = {
          ...(baseCreationData || {}),
          moderation,
        };

        const thumbName = `thumb-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.jpg`;
        const thumbPath = path.join(outDir, thumbName);

        const tPrep = nowMs();
        const durationSecondsRaw = await getVideoDurationSeconds(decryptedPath);
        const durationSeconds = Math.max(0, Math.floor(Number(durationSecondsRaw) || 0));
        const durationText = formatDurationText(durationSeconds);

        await generateThumbnailWithDuration(decryptedPath, thumbPath, durationSeconds);
        logStage(reqId, "duration + thumb ready", {
          ms: elapsed(tPrep),
          durationSeconds,
        });

        const tHls = nowMs();
        const hlsBase = `endpoint-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`;
        await generateHlsVOD(decryptedPath, hlsDir);

        const localMaster = path.join(hlsDir, "master.m3u8");
        if (!fs.existsSync(localMaster)) {
          throw new Error("HLS export failed: master.m3u8 not created");
        }
        logStage(reqId, "hls generated", { ms: elapsed(tHls) });

        const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
        const assetsBucket = process.env.S3_ASSETS_BUCKET;
        const hlsKeyPrefix = `hls/${userId}/${hlsBase}`;
        const thumbKey = `thumbs/${userId}/${thumbName}`;

        const assetsS3 = makeAssetsS3Client();

        const tUpload = nowMs();
        await Promise.all([
          uploadDirToS3Concurrent({
            uploadFileToS3,
            bucket: uploadsBucket,
            localDir: hlsDir,
            keyPrefix: hlsKeyPrefix,
            concurrency: 6,
          }),
          retry(() =>
            uploadFileToAssetsBucket({
              assetsS3,
              bucket: assetsBucket,
              key: thumbKey,
              filePath: thumbPath,
              contentType: "image/jpeg",
            })
          ),
        ]);
        logStage(reqId, "uploads complete", { ms: elapsed(tUpload) });

        const hlsMasterKey = `${hlsKeyPrefix}/master.m3u8`;

        const tDb = nowMs();
        const ins = await pool.query(
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
            thumb,
            duration_text,
            duration,
            views,
            tags,
            creation_data,
            moderation_check
          )
          VALUES ($1, $2, $3, 'Other', $4, 'video', 'public', $5, $6, $7, $8, 0, $9, $10::jsonb, $11)
          RETURNING id
          `,
          [
            userId,
            title,
            description,
            visibility,
            hlsMasterKey,
            thumbKey,
            durationText,
            durationSeconds,
            tags,
            JSON.stringify(creationData || {}),
            moderationCheck,
          ]
        );
        logStage(reqId, "db insert complete", { ms: elapsed(tDb) });

        const newVideoId = ins.rows[0].id;

        cleanup();
        logStage(reqId, "done", { totalMs: elapsed(startedAt) });

        return res.json({
          ok: true,
          videoId: newVideoId,
          filename: hlsMasterKey,
          thumb: thumbKey,
          duration: durationSeconds,
          durationText,
          creationData,
          moderationCheck,
          moderationStatus: moderation.status,
          moderationSources: moderation.all_source_urls.length,
          encryptionMode: decryptResult.mode,
          ms: elapsed(startedAt),
        });
      } catch (e) {
        console.error(`[endpoint-publish ${reqId}] error:`, e);
        cleanup();
        return res.status(500).json({
          ok: false,
          error: e?.message || "Endpoint publish failed",
        });
      }
    }
  );
}