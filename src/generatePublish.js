// server/src/generatePublish.js
import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import { pipeline } from "stream/promises";
import {
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { s3 } from "./aws/s3Client.js";

/* ============================================================
   GLOBAL ERROR TRAPS (helps catch weird crashes causing 502)
============================================================ */
process.on("unhandledRejection", (err) => {
  console.error("🔥 UNHANDLED REJECTION:", err?.stack || err);
});

process.on("uncaughtException", (err) => {
  console.error("🔥 UNCAUGHT EXCEPTION:", err?.stack || err);
});

/* ============================================================
   UTIL: run external command with diagnostics
============================================================ */
function runCmd(cmd, args, { cwd } = {}) {
  const printable = args.map((a) => JSON.stringify(a)).join(" ");
  console.log(`▶️ Running: ${cmd} ${printable}${cwd ? ` (cwd=${cwd})` : ""}`);

  return new Promise((resolve, reject) => {
    const start = Date.now();
    const p = spawn(cmd, args, { windowsHide: true, cwd });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", (e) => {
      console.error(`❌ Spawn error for ${cmd}:`, e);
      reject(e);
    });

    p.on("close", (code, signal) => {
      const ms = Date.now() - start;
      console.log(`⏱ ${cmd} exited code=${code} signal=${signal} in ${ms}ms`);

      if (code === 0) return resolve({ out, err, ms });

      console.error(`❌ ${cmd} FAILED`);
      console.error("stderr (first 2000 chars):");
      console.error(String(err || "").slice(0, 2000));
      reject(new Error(err || `${cmd} exited with code ${code}`));
    });
  });
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

/**
 * Normalize timeline clips.
 * Expects clips like:
 * { kind: "video"|"audio", track, videoId, start, in, out, gain }
 */
function normClips(raw, { kindDefault = null } = {}) {
  const arr = Array.isArray(raw) ? raw : [];
  return arr
    .map((c) => ({
      kind: String(c.kind ?? kindDefault ?? "").trim().toLowerCase() || null,
      track: Number.isFinite(Number(c.track)) ? Number(c.track) : 0,
      videoId: String(c.videoId ?? c.id ?? ""),
      start: Number(c.start ?? 0),
      in: Number(c.in ?? 0),
      out: Number(c.out ?? 0),
      gain: c.gain == null ? 1 : Number(c.gain),
    }))
    .filter(
      (c) =>
        c.videoId &&
        Number.isFinite(c.start) &&
        Number.isFinite(c.in) &&
        Number.isFinite(c.out) &&
        c.out > c.in &&
        c.start >= 0
    )
    .map((c) => ({
      ...c,
      track: clamp(Number(c.track) || 0, 0, 10),
      gain: Number.isFinite(c.gain) ? c.gain : 1,
    }))
    .sort((a, b) => a.start - b.start);
}

function splitTimeline(timeline) {
  const all = normClips(timeline, { kindDefault: "video" });
  const videoClips = all
    .filter((c) => (c.kind || "video") === "video")
    .map((c) => ({ ...c, kind: "video", track: 0 })); // video lane = single
  const audioClips = all
    .filter((c) => c.kind === "audio")
    .map((c) => ({ ...c, kind: "audio" }));
  return { all, videoClips, audioClips };
}

function timelineDurationSeconds(videoClips) {
  let maxEnd = 0;
  for (const c of videoClips) {
    const end =
      (Number(c.start) || 0) + ((Number(c.out) || 0) - (Number(c.in) || 0));
    if (end > maxEnd) maxEnd = end;
  }
  return Math.max(0, maxEnd);
}

async function hasAudioStream(inputPath) {
  try {
    const { out } = await runCmd("ffprobe", [
      "-v",
      "error",
      "-select_streams",
      "a:0",
      "-show_entries",
      "stream=codec_type",
      "-of",
      "default=noprint_wrappers=1:nokey=1",
      inputPath,
    ]);
    return String(out || "").trim() === "audio";
  } catch {
    return false;
  }
}

async function extractThumbnail({ inputPath, outPath, atSeconds }) {
  const t = Math.max(0, Number(atSeconds) || 0);
  await runCmd("ffmpeg", [
    "-y",
    "-hide_banner",
    "-loglevel",
    "error",
    "-ss",
    String(t),
    "-i",
    inputPath,
    "-frames:v",
    "1",
    "-vf",
    "scale=1280:-2",
    "-q:v",
    "2",
    outPath,
  ]);
}

/* ============================================================
   S3 HELPERS
============================================================ */
async function downloadS3ToFile({ bucket, key, outPath }) {
  console.log("⬇️ S3 download:", { bucket, key, outPath });
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  if (!resp?.Body) throw new Error(`S3 download failed for ${key}`);
  await pipeline(resp.Body, fs.createWriteStream(outPath));
  return outPath;
}

async function downloadS3PrefixToDir({ bucket, prefix, outDir }) {
  console.log("⬇️ S3 download prefix:", { bucket, prefix, outDir });
  fs.mkdirSync(outDir, { recursive: true });
  let ContinuationToken = undefined;

  while (true) {
    const listed = await s3.send(
      new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken })
    );

    const items = listed?.Contents || [];
    for (const obj of items) {
      const k = obj.Key;
      if (!k || k.endsWith("/")) continue;

      const rel = k.slice(prefix.length);
      const localPath = path.join(outDir, rel);
      fs.mkdirSync(path.dirname(localPath), { recursive: true });

      await downloadS3ToFile({ bucket, key: k, outPath: localPath });
    }

    if (!listed.IsTruncated) break;
    ContinuationToken = listed.NextContinuationToken;
  }
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

async function uploadDirToS3({ uploadFileToS3, bucket, localDir, keyPrefix }) {
  console.log("⬆️ Upload dir to S3:", { bucket, localDir, keyPrefix });
  const files = listFilesRecursive(localDir);
  for (const filePath of files) {
    const rel = path.relative(localDir, filePath).split(path.sep).join("/");
    const key = `${keyPrefix.replace(/\/+$/g, "")}/${rel}`;
    const ext = path.extname(filePath);
    await uploadFileToS3({
      bucket,
      key,
      filePath,
      contentType: contentTypeForExt(ext),
    });
  }
}

// Dedicated uploader for ASSETS bucket (handles different region/endpoint)
function makeAssetsS3Client() {
  const region =
    process.env.S3_ASSETS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;

  if (!region) {
    throw new Error(
      "Missing env S3_ASSETS_REGION (or AWS_REGION) for assets bucket uploads"
    );
  }

  const endpoint = process.env.S3_ASSETS_ENDPOINT || undefined;

  return new S3Client({
    region,
    ...(endpoint ? { endpoint } : {}),
  });
}

async function uploadFileToAssetsBucket({ assetsS3, bucket, key, filePath, contentType }) {
  console.log("⬆️ Upload thumb to assets bucket:", { bucket, key, filePath });
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

/* ============================================================
   FFMPEG FILTER GRAPH
============================================================ */
function buildFilterVideoAndAudio({
  videoClips,
  audioClips,
  idToInputIndex,
  audioPresentByInputIndex,
  totalDur,
}) {
  const parts = [];

  // VIDEO: concat in order (ignores gaps)
  const vLabels = [];
  for (let j = 0; j < videoClips.length; j++) {
    const c = videoClips[j];
    const idx = idToInputIndex.get(String(c.videoId));
    if (idx == null) throw new Error(`Missing input index for videoId ${c.videoId}`);

    const dur = Math.max(0, Number(c.out) - Number(c.in));
    parts.push(`[${idx}:v]trim=start=${c.in}:duration=${dur},setpts=PTS-STARTPTS[v${j}]`);
    vLabels.push(`[v${j}]`);
  }
  parts.push(`${vLabels.join("")}concat=n=${videoClips.length}:v=1:a=0[vout]`);

  // AUDIO: layered mix positioned by start
  const safeTotal = Math.max(0.01, Number(totalDur) || 0);

  if (!audioClips.length) {
    parts.push(`anullsrc=r=48000:cl=stereo,atrim=0:${safeTotal},asetpts=PTS-STARTPTS[aout]`);
    return parts.join(";");
  }

  const aLabels = [];
  for (let k = 0; k < audioClips.length; k++) {
    const c = audioClips[k];
    const idx = idToInputIndex.get(String(c.videoId));
    if (idx == null) throw new Error(`Missing input index for audio videoId ${c.videoId}`);

    const dur = Math.max(0, Number(c.out) - Number(c.in));
    const delayMs = Math.max(0, Math.round((Number(c.start) || 0) * 1000));
    const gain = Number.isFinite(Number(c.gain)) ? Number(c.gain) : 1;

    if (audioPresentByInputIndex.get(idx)) {
      parts.push(
        `[${idx}:a]atrim=start=${c.in}:duration=${dur},asetpts=PTS-STARTPTS,` +
          `volume=${gain},adelay=${delayMs}|${delayMs}[a${k}]`
      );
    } else {
      // if the input has no audio stream, feed silence of the same duration
      parts.push(`aevalsrc=0:d=${dur},adelay=${delayMs}|${delayMs}[a${k}]`);
    }
    aLabels.push(`[a${k}]`);
  }

  parts.push(
    `${aLabels.join("")}amix=inputs=${audioClips.length}:dropout_transition=0,` +
      `atrim=0:${safeTotal},asetpts=PTS-STARTPTS[aout]`
  );

  return parts.join(";");
}

/* ============================================================
   STEP SELECTOR
============================================================ */
function getDebugStep(req) {
  const qp = Number(req.query?.step);
  if (Number.isFinite(qp)) return qp;

  const hdr = Number(req.headers["x-debug-step"]);
  if (Number.isFinite(hdr)) return hdr;

  const env = Number(process.env.GENERATE_PUBLISH_STEP);
  if (Number.isFinite(env)) return env;

  return 4; // default = run everything
}

function stepShouldRun(currentStep, requestedStep) {
  // requestedStep is "run up to this step"
  return currentStep <= requestedStep;
}

/* ============================================================
   MAIN ROUTE (Progressive rebuild)
============================================================ */
export function registerGeneratePublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  if (!pool) throw new Error("registerGeneratePublish: missing pool");
  if (!requireAuth) throw new Error("registerGeneratePublish: missing requireAuth");
  if (!uploadFileToS3) throw new Error("registerGeneratePublish: missing uploadFileToS3");

  app.post("/api/generate/publish", requireAuth, async (req, res) => {
    const requestStart = Date.now();
    const requestedStep = getDebugStep(req);

    console.log("====================================");
    console.log("🚀 PUBLISH START");
    console.log("Requested step:", requestedStep);
    console.log("Origin:", req.headers.origin);
    console.log("User:", req.user?.id);
    console.log("Body keys:", Object.keys(req.body || {}));
    console.log("====================================");

    const userId = Number(req.user?.id);
    if (!Number.isFinite(userId) || userId <= 0) {
      return res.status(401).json({ error: "Not logged in" });
    }

    // temp workspace
    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-export-"));
    const inputDir = path.join(tmpRoot, "inputs");
    const outDir = path.join(tmpRoot, "out");
    fs.mkdirSync(inputDir, { recursive: true });
    fs.mkdirSync(outDir, { recursive: true });

    const cleanup = () => {
      try {
        fs.rmSync(tmpRoot, { recursive: true, force: true });
      } catch (e) {
        console.error("cleanup failed:", e?.message || e);
      }
    };

    // Keep a debug payload you can return at any step
    const debug = {
      requestedStep,
      ms: {},
      tmpRoot,
      inputDir,
      outDir,
      buckets: {},
      sources: {},
      artifacts: {},
    };

    try {
      /* ============================================================
         STEP 0: validation + ffmpeg presence
      ============================================================ */
      const t0 = Date.now();
      const {
        title,
        description = "",
        tags = "",
        visibility = "public",
        timelineName = "Timeline",
        timeline,
      } = req.body || {};

      if (!String(title || "").trim()) {
        cleanup();
        return res.status(400).json({ error: "Title required" });
      }

      if (!Array.isArray(timeline) || timeline.length === 0) {
        cleanup();
        return res.status(400).json({ error: "Timeline empty" });
      }

      const { all, videoClips, audioClips } = splitTimeline(timeline);
      const totalDur = timelineDurationSeconds(videoClips);

      debug.sources.clipCounts = {
        all: all.length,
        video: videoClips.length,
        audio: audioClips.length,
      };
      debug.sources.totalDur = totalDur;

      console.log("✅ STEP 0 ok");
      console.log("📊 Clips:", debug.sources.clipCounts);
      console.log("🕒 Total seconds:", totalDur);

      await runCmd("ffmpeg", ["-version"]);
      debug.ms.step0 = Date.now() - t0;

      if (!stepShouldRun(0, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 0, debug });
      }

      /* ============================================================
         STEP 1: required env/buckets present
      ============================================================ */
      const t1 = Date.now();
      const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
      const assetsBucket = process.env.S3_ASSETS_BUCKET;

      if (!uploadsBucket) throw new Error("Missing env S3_UPLOADS_BUCKET");
      if (!assetsBucket) throw new Error("Missing env S3_ASSETS_BUCKET");

      debug.buckets.uploadsBucket = uploadsBucket;
      debug.buckets.assetsBucket = assetsBucket;

      console.log("✅ STEP 1 ok (buckets present)", debug.buckets);
      debug.ms.step1 = Date.now() - t1;

      if (!stepShouldRun(1, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 1, debug });
      }

      /* ============================================================
         STEP 2: fetch DB filenames for all referenced sources
      ============================================================ */
      const t2 = Date.now();
      const sourceIds = Array.from(new Set([...videoClips, ...audioClips].map((c) => String(c.videoId))));
      debug.sources.uniqueIds = sourceIds.length;

      const q = await pool.query(
        `SELECT id, filename FROM videos WHERE id::text = ANY($1::text[])`,
        [sourceIds]
      );

      debug.sources.dbRowCount = q.rowCount;

      const byId = new Map(q.rows.map((r) => [String(r.id), String(r.filename || "")]));
      for (const id of sourceIds) {
        const key = byId.get(id);
        if (!key) throw new Error(`Unknown/missing source for videoId ${id}`);
      }

      console.log("✅ STEP 2 ok (DB sources fetched)", { dbRowCount: q.rowCount });
      debug.ms.step2 = Date.now() - t2;

      if (!stepShouldRun(2, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 2, debug });
      }

      /* ============================================================
         STEP 3: download sources to disk + detect audio streams
      ============================================================ */
      const t3 = Date.now();

      const inputPaths = [];
      const idToInputIndex = new Map();

      for (let i = 0; i < sourceIds.length; i++) {
        const id = sourceIds[i];
        const key = byId.get(id);

        if (key.endsWith("/master.m3u8")) {
          const prefix = key.replace(/master\.m3u8$/i, "");
          const localHlsDir = path.join(inputDir, `hls-${i}-${id}`);
          await downloadS3PrefixToDir({ bucket: uploadsBucket, prefix, outDir: localHlsDir });

          const localMaster = path.join(localHlsDir, "master.m3u8");
          if (!fs.existsSync(localMaster)) {
            throw new Error(`Downloaded HLS missing master.m3u8 for ${id}`);
          }

          idToInputIndex.set(id, inputPaths.length);
          inputPaths.push(localMaster);
        } else {
          const ext = path.extname(key) || ".mp4";
          const localPath = path.join(inputDir, `src-${i}-${id}${ext}`);
          await downloadS3ToFile({ bucket: uploadsBucket, key, outPath: localPath });

          idToInputIndex.set(id, inputPaths.length);
          inputPaths.push(localPath);
        }
      }

      const audioPresentByInputIndex = new Map();
      for (let i = 0; i < inputPaths.length; i++) {
        audioPresentByInputIndex.set(i, await hasAudioStream(inputPaths[i]));
      }

      debug.sources.downloadedInputs = inputPaths.length;
      debug.sources.audioPresent = Object.fromEntries([...audioPresentByInputIndex.entries()]);

      console.log("✅ STEP 3 ok (downloaded inputs)", {
        inputs: inputPaths.length,
        audioPresent: debug.sources.audioPresent,
      });

      debug.ms.step3 = Date.now() - t3;

      if (!stepShouldRun(3, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 3, debug });
      }

      /* ============================================================
         STEP 4: build filter graph
      ============================================================ */
      const t4 = Date.now();

      const filter = buildFilterVideoAndAudio({
        videoClips,
        audioClips,
        idToInputIndex,
        audioPresentByInputIndex,
        totalDur,
      });

      debug.artifacts.filterPreview = String(filter).slice(0, 500);

      console.log("✅ STEP 4 ok (filter built)");
      debug.ms.step4 = Date.now() - t4;

      if (!stepShouldRun(4, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 4, debug });
      }

      /* ============================================================
         STEP 5: render MP4 intermediate
      ============================================================ */
      const t5 = Date.now();

      const mp4Name = `export-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.mp4`;
      const mp4Path = path.join(outDir, mp4Name);

      const mp4Args = [];
      for (const p of inputPaths) mp4Args.push("-i", p);

      mp4Args.push(
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-filter_complex",
        filter,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "22",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        mp4Path
      );

      await runCmd("ffmpeg", mp4Args);

      if (!fs.existsSync(mp4Path) || fs.statSync(mp4Path).size < 1024) {
        throw new Error("MP4 render failed (missing or too small)");
      }

      debug.artifacts.mp4Path = mp4Path;
      debug.artifacts.mp4Size = fs.statSync(mp4Path).size;

      console.log("✅ STEP 5 ok (mp4 rendered)", { size: debug.artifacts.mp4Size });
      debug.ms.step5 = Date.now() - t5;

      if (!stepShouldRun(5, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 5, debug });
      }

      /* ============================================================
         STEP 6: generate HLS VOD from MP4
      ============================================================ */
      const t6 = Date.now();

      const hlsBase = `gen-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`;
      const hlsLocalDir = path.join(outDir, `hls-${hlsBase}`);
      fs.mkdirSync(hlsLocalDir, { recursive: true });

      const localMaster = path.join(hlsLocalDir, "master.m3u8");
      const localSegPattern = path.join(hlsLocalDir, "seg-%05d.ts");

      await runCmd("ffmpeg", [
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        mp4Path,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "22",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-f",
        "hls",
        "-hls_time",
        "4",
        "-hls_playlist_type",
        "vod",
        "-hls_segment_filename",
        localSegPattern,
        localMaster,
      ]);

      if (!fs.existsSync(localMaster)) {
        throw new Error("HLS export failed: master.m3u8 was not created");
      }

      debug.artifacts.hlsLocalDir = hlsLocalDir;
      debug.artifacts.hlsBase = hlsBase;

      console.log("✅ STEP 6 ok (hls created)", { hlsLocalDir });
      debug.ms.step6 = Date.now() - t6;

      if (!stepShouldRun(6, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 6, debug });
      }

      /* ============================================================
         STEP 7: extract thumbnail from MP4
      ============================================================ */
      const t7 = Date.now();

      const safeTotal = Math.max(0.01, Number(totalDur) || 0);
      const mid = clamp(safeTotal / 2, 0, Math.max(0, safeTotal - 0.1));

      const thumbName = `thumb-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.jpg`;
      const thumbPath = path.join(outDir, thumbName);

      await extractThumbnail({
        inputPath: mp4Path,
        outPath: thumbPath,
        atSeconds: mid,
      });

      if (!fs.existsSync(thumbPath) || fs.statSync(thumbPath).size < 1000) {
        throw new Error("Thumbnail extraction failed (thumb missing or too small)");
      }

      debug.artifacts.thumbPath = thumbPath;
      debug.artifacts.thumbSize = fs.statSync(thumbPath).size;

      console.log("✅ STEP 7 ok (thumb extracted)", { size: debug.artifacts.thumbSize });
      debug.ms.step7 = Date.now() - t7;

      if (!stepShouldRun(7, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 7, debug });
      }

      /* ============================================================
         STEP 8: upload HLS dir (uploads bucket) + thumb (assets bucket)
      ============================================================ */
      const t8 = Date.now();

      const hlsKeyPrefix = `hls/${userId}/${hlsBase}`;
      await uploadDirToS3({
        uploadFileToS3,
        bucket: uploadsBucket,
        localDir: hlsLocalDir,
        keyPrefix: hlsKeyPrefix,
      });

      const thumbKey = `thumbs/${userId}/${thumbName}`;
      const assetsS3 = makeAssetsS3Client();
      await uploadFileToAssetsBucket({
        assetsS3,
        bucket: assetsBucket,
        key: thumbKey,
        filePath: thumbPath,
        contentType: "image/jpeg",
      });

      debug.artifacts.hlsKeyPrefix = hlsKeyPrefix;
      debug.artifacts.thumbKey = thumbKey;

      console.log("✅ STEP 8 ok (uploads complete)", {
        hlsKeyPrefix,
        thumbKey,
      });

      debug.ms.step8 = Date.now() - t8;

      if (!stepShouldRun(8, requestedStep)) {
        cleanup();
        return res.json({ ok: true, step: 8, debug });
      }

      /* ============================================================
         STEP 9: insert DB row + return new videoId
      ============================================================ */
      const t9 = Date.now();

      const hlsMasterKey = `${hlsKeyPrefix}/master.m3u8`;

      const allowedVis = new Set(["public", "private", "unlisted"]);
      const vis = allowedVis.has(String(visibility).toLowerCase())
        ? String(visibility).toLowerCase()
        : "public";

      const tagsArr = Array.from(
        new Set(
          String(tags || "")
            .split(",")
            .map((t) => t.trim().toLowerCase())
            .filter(Boolean)
            .slice(0, 30)
        )
      );

      const ins = await pool.query(
        `
        INSERT INTO videos (
          user_id, title, description, category, visibility,
          media_type, asset_scope,
          filename, thumb, duration_text, views, tags
        )
        VALUES ($1, $2, $3, 'Other', $4, 'video', 'public', $5, $6, NULL, 0, $7)
        RETURNING id
        `,
        [
          userId,
          String(title).trim(),
          String(description || "").trim(),
          vis,
          hlsMasterKey,
          thumbKey,
          tagsArr,
        ]
      );

      const newVideoId = ins.rows[0].id;
      debug.artifacts.newVideoId = newVideoId;

      console.log("✅ STEP 9 ok (DB inserted)", { newVideoId });
      debug.ms.step9 = Date.now() - t9;

      console.log(`🎉 Publish completed in ${Date.now() - requestStart}ms`);
      cleanup();

      return res.json({ ok: true, step: 9, videoId: newVideoId, timelineName, debug });
    } catch (e) {
      console.error("💥 PUBLISH ERROR:");
      console.error(e?.stack || e);

      // best-effort: never throw inside cleanup
      cleanup();

      return res.status(500).json({
        ok: false,
        error: e?.message || "Publish failed",
        debug,
      });
    }
  });
}