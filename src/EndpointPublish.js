import fs from "fs";
import path from "path";
import os from "os";
import crypto from "crypto";
import { spawn } from "child_process";
import multer from "multer";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { s3 } from "./aws/s3Client.js";
import { getOrCreateUserMediaKey, mediaKeyBase64ToBuffer } from "./mediaKeys.js";

/* ============================================================
   UTIL
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
    "scale=1280:-2",
    "-q:v",
    "2",
    thumbPath,
  ]);
}

async function generateThumbnailHalfwayWithFallback(videoPath, thumbPath) {
  const dur = await getVideoDurationSeconds(videoPath);

  let candidates = [];
  if (dur && dur > 0) {
    const half = clamp(Math.floor(dur * 0.5), 1, Math.max(1, Math.floor(dur - 1)));
    candidates = [half, 30, 10, 3, 1]
      .map((t) => clamp(Number(t), 0, Math.max(0, Math.floor(dur - 0.25))))
      .filter((t) => Number.isFinite(t) && t >= 0);

    candidates = Array.from(new Set(candidates));
  } else {
    candidates = [30, 10, 3, 1];
  }

  let lastErr = null;

  for (const t of candidates) {
    try {
      await generateThumbnailAtSecond(videoPath, thumbPath, t);
      return { ok: true, usedSecond: t, duration: dur };
    } catch (e) {
      lastErr = e;
      try {
        if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
      } catch {}
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
    "-i",
    inputPath,
    "-vf",
    "scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p",
    "-c:v",
    "libx264",
    "-preset",
    "veryfast",
    "-crf",
    "22",
    "-pix_fmt",
    "yuv420p",
    "-profile:v",
    "main",
    "-level",
    "4.0",
    "-colorspace",
    "bt709",
    "-color_primaries",
    "bt709",
    "-color_trc",
    "bt709",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-ac",
    "2",
    "-f",
    "hls",
    "-hls_time",
    "6",
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

function parseBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  const v = String(value ?? "").trim().toLowerCase();
  if (["true", "1", "yes", "y"].includes(v)) return true;
  if (["false", "0", "no", "n", ""].includes(v)) return false;
  return fallback;
}

function parseCreationData(raw) {
  if (raw == null) return {};

  if (typeof raw === "object" && !Array.isArray(raw)) {
    return raw;
  }

  const text = String(raw).trim();
  if (!text) return {};

  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
    return { value: parsed };
  } catch {
    return { value: text };
  }
}

function decryptFileAes256Gcm({ encryptedPath, outputPath, keyBuffer, ivB64, authTagB64 }) {
  if (!ivB64) throw new Error("Missing iv");
  if (!authTagB64) throw new Error("Missing authTag");

  const iv = Buffer.from(String(ivB64), "base64");
  const authTag = Buffer.from(String(authTagB64), "base64");

  if (iv.length !== 12) {
    throw new Error("IV must be 12 bytes (base64 encoded)");
  }

  if (authTag.length !== 16) {
    throw new Error("Auth tag must be 16 bytes (base64 encoded)");
  }

  const decipher = crypto.createDecipheriv("aes-256-gcm", keyBuffer, iv);
  decipher.setAuthTag(authTag);

  return new Promise((resolve, reject) => {
    const input = fs.createReadStream(encryptedPath);
    const output = fs.createWriteStream(outputPath);

    input.on("error", reject);
    output.on("error", reject);
    output.on("close", resolve);

    input.pipe(decipher).pipe(output);
  });
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
    },
  });

  app.post(
    "/api/endpoint/publish",
    requireAuth,
    upload.single("media"),
    async (req, res) => {
      const startedAt = Date.now();
      const userId = Number(req.user.id);

      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-endpoint-publish-"));
      const decryptedDir = path.join(tmpRoot, "decrypted");
      const outDir = path.join(tmpRoot, "out");
      const hlsDir = path.join(outDir, "hls");
      fs.mkdirSync(decryptedDir, { recursive: true });
      fs.mkdirSync(outDir, { recursive: true });
      fs.mkdirSync(hlsDir, { recursive: true });

      const cleanup = () => {
        try {
          fs.rmSync(tmpRoot, { recursive: true, force: true });
        } catch {}
        try {
          if (req.file?.path && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        } catch {}
      };

      try {
        const title = String(req.body?.title || "").trim();
        const description = String(req.body?.description || "").trim();
        const visibility = String(req.body?.visibility || "public").trim().toLowerCase();
        const tags = parseTags(req.body?.tags);
        const iv = String(req.body?.iv || "").trim();
        const authTag = String(req.body?.authTag || "").trim();

        const creationData = parseCreationData(req.body?.creationData);
        const moderationCheck = false;

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
          return res.status(400).json({ error: "Visibility must be public, private, or unlisted" });
        }

        if (!process.env.S3_UPLOADS_BUCKET) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_UPLOADS_BUCKET" });
        }

        if (!process.env.S3_ASSETS_BUCKET) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_ASSETS_BUCKET" });
        }

        const mediaKey = await getOrCreateUserMediaKey(userId);
        const keyBuffer = mediaKeyBase64ToBuffer(mediaKey);

        const originalExt =
          path.extname(req.body?.originalFilename || "").toLowerCase() ||
          path.extname(req.file.originalname || "").toLowerCase() ||
          ".mp4";

        const decryptedPath = path.join(
          decryptedDir,
          `decrypted-${Date.now()}-${crypto.randomBytes(6).toString("hex")}${originalExt || ".mp4"}`
        );

        await decryptFileAes256Gcm({
          encryptedPath: req.file.path,
          outputPath: decryptedPath,
          keyBuffer,
          ivB64: iv,
          authTagB64: authTag,
        });

        const durationSecondsRaw = await getVideoDurationSeconds(decryptedPath);
        const durationSeconds = Math.max(0, Math.floor(Number(durationSecondsRaw) || 0));
        const durationText = formatDurationText(durationSeconds);

        const thumbName = `thumb-${Date.now()}-${crypto.randomBytes(6).toString("hex")}.jpg`;
        const thumbPath = path.join(outDir, thumbName);

        await generateThumbnailHalfwayWithFallback(decryptedPath, thumbPath);

        const hlsBase = `endpoint-${Date.now()}-${crypto.randomBytes(6).toString("hex")}`;
        await generateHlsVOD(decryptedPath, hlsDir);

        const localMaster = path.join(hlsDir, "master.m3u8");
        if (!fs.existsSync(localMaster)) {
          throw new Error("HLS export failed: master.m3u8 not created");
        }

        const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
        const assetsBucket = process.env.S3_ASSETS_BUCKET;

        const hlsKeyPrefix = `hls/${userId}/${hlsBase}`;
        await uploadDirToS3({
          uploadFileToS3,
          bucket: uploadsBucket,
          localDir: hlsDir,
          keyPrefix: hlsKeyPrefix,
        });

        const assetsS3 = makeAssetsS3Client();
        const thumbKey = `thumbs/${userId}/${thumbName}`;
        await uploadFileToAssetsBucket({
          assetsS3,
          bucket: assetsBucket,
          key: thumbKey,
          filePath: thumbPath,
          contentType: "image/jpeg",
        });

        const hlsMasterKey = `${hlsKeyPrefix}/master.m3u8`;

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

        const newVideoId = ins.rows[0].id;

        cleanup();

        return res.json({
          ok: true,
          videoId: newVideoId,
          filename: hlsMasterKey,
          thumb: thumbKey,
          duration: durationSeconds,
          durationText,
          creationData,
          moderationCheck,
          ms: Date.now() - startedAt,
        });
      } catch (e) {
        console.error("POST /api/endpoint/publish error:", e);
        cleanup();
        return res.status(500).json({
          ok: false,
          error: e?.message || "Endpoint publish failed",
        });
      }
    }
  );
}