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
function runCmd(cmd, args) {
  console.log(`▶️ Running: ${cmd} ${args.join(" ")}`);

  return new Promise((resolve, reject) => {
    const start = Date.now();
    const p = spawn(cmd, args, { windowsHide: true });

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

      if (code === 0) {
        resolve({ out, err });
      } else {
        console.error(`❌ ${cmd} FAILED`);
        console.error("stderr (first 2000 chars):");
        console.error(String(err || "").slice(0, 2000));
        reject(new Error(err || `${cmd} exited with code ${code}`));
      }
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

function timelineDurationSeconds(videoClips) {
  let maxEnd = 0;
  for (const c of videoClips) {
    const end = (Number(c.start) || 0) + ((Number(c.out) || 0) - (Number(c.in) || 0));
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
   S3 HELPERS (used later in the rebuild steps)
============================================================ */
async function downloadS3ToFile({ bucket, key, outPath }) {
  const resp = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  if (!resp?.Body) throw new Error(`S3 download failed for ${key}`);
  await pipeline(resp.Body, fs.createWriteStream(outPath));
  return outPath;
}

async function downloadS3PrefixToDir({ bucket, prefix, outDir }) {
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
   MAIN ROUTE (Progressive rebuild scaffold)
============================================================ */
export function registerGeneratePublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  if (!pool) throw new Error("registerGeneratePublish: missing pool");
  if (!requireAuth) throw new Error("registerGeneratePublish: missing requireAuth");
  if (!uploadFileToS3) throw new Error("registerGeneratePublish: missing uploadFileToS3");

  app.post("/api/generate/publish", requireAuth, async (req, res) => {
    const requestStart = Date.now();

    console.log("====================================");
    console.log("🚀 PUBLISH START");
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

    try {
      /* ============================================================
         STEP 0: minimal validation + ffmpeg sanity (CURRENT WORKING)
         Keep this step stable before enabling the next ones.
      ============================================================ */
      const { title, timeline } = req.body || {};

      if (!String(title || "").trim()) {
        cleanup();
        return res.status(400).json({ error: "Title required" });
      }

      if (!Array.isArray(timeline) || timeline.length === 0) {
        cleanup();
        return res.status(400).json({ error: "Timeline empty" });
      }

      const clips = normClips(timeline);
      const totalDur = timelineDurationSeconds(clips);

      console.log("✅ Validation passed");
      console.log("📊 Clips normalized:", clips.length);
      console.log("🕒 Timeline total seconds:", totalDur);

      console.log("🎬 Testing ffmpeg pipeline...");
      await runCmd("ffmpeg", ["-version"]);
      console.log("✅ ffmpeg exists");

      console.log(`🎉 Publish handler completed in ${Date.now() - requestStart}ms`);
      cleanup();

      // NOTE: For now, we return debug:true.
      // As you progressively rebuild, keep the same response shape or expand it.
      return res.json({ ok: true, debug: true });

      /* ============================================================
         NEXT STEPS (Enable one at a time)
         - Uncomment STEP 1, deploy, test.
         - Then STEP 2, deploy, test.
         - etc.
      ============================================================ */

      // // STEP 1: confirm buckets exist / env present
      // const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
      // const assetsBucket = process.env.S3_ASSETS_BUCKET;
      // if (!uploadsBucket) throw new Error("Missing env S3_UPLOADS_BUCKET");
      // if (!assetsBucket) throw new Error("Missing env S3_ASSETS_BUCKET");
      // console.log("🪣 Buckets OK:", { uploadsBucket, assetsBucket });

      // // STEP 2: fetch DB filenames for sources referenced in timeline
      // // (you’ll likely need to split video/audio clips again)
      // // const sourceIds = Array.from(new Set(clips.map(c => String(c.videoId))));
      // // const q = await pool.query(`SELECT id, filename FROM videos WHERE id::text = ANY($1::text[])`, [sourceIds]);
      // // console.log("🗃️ DB sources fetched:", q.rowCount);

      // // STEP 3: download one source (single) from S3 into inputDir and run ffprobe
      // // STEP 4: build filter graph
      // // STEP 5: render mp4
      // // STEP 6: generate hls
      // // STEP 7: extract thumb
      // // STEP 8: upload HLS dir + thumb
      // // STEP 9: insert DB row, return { ok:true, videoId }

    } catch (e) {
      console.error("💥 PUBLISH ERROR:");
      console.error(e?.stack || e);

      cleanup();

      return res.status(500).json({
        error: e?.message || "Publish failed",
      });
    }
  });
}