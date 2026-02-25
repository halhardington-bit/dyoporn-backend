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

function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { windowsHide: true });

    let out = "";
    let err = "";

    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", (d) => (err += d.toString()));

    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve({ out, err });
      else reject(new Error(err || `${cmd} exited with code ${code}`));
    });
  });
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function normClips(raw, { kindDefault = null } = {}) {
  const arr = Array.isArray(raw) ? raw : [];
  return arr
    .map((c) => ({
      kind: String(c.kind ?? kindDefault ?? "").trim().toLowerCase() || null, // "video" | "audio"
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
    const end =
      (Number(c.start) || 0) +
      ((Number(c.out) || 0) - (Number(c.in) || 0));
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
    const dur = Math.max(0, Number(c.out) - Number(c.in));

    parts.push(
      `[${idx}:v]trim=start=${c.in}:duration=${dur},setpts=PTS-STARTPTS[v${j}]`
    );
    vLabels.push(`[v${j}]`);
  }
  parts.push(`${vLabels.join("")}concat=n=${videoClips.length}:v=1:a=0[vout]`);

  // AUDIO: layered mix positioned by start
  const safeTotal = Math.max(0.01, Number(totalDur) || 0);

  if (!audioClips.length) {
    parts.push(
      `anullsrc=r=48000:cl=stereo,atrim=0:${safeTotal},asetpts=PTS-STARTPTS[aout]`
    );
    return parts.join(";");
  }

  const aLabels = [];
  for (let k = 0; k < audioClips.length; k++) {
    const c = audioClips[k];
    const idx = idToInputIndex.get(String(c.videoId));
    const dur = Math.max(0, Number(c.out) - Number(c.in));
    const delayMs = Math.max(0, Math.round((Number(c.start) || 0) * 1000));
    const gain = Number.isFinite(Number(c.gain)) ? Number(c.gain) : 1;

    if (audioPresentByInputIndex.get(idx)) {
      parts.push(
        `[${idx}:a]atrim=start=${c.in}:duration=${dur},asetpts=PTS-STARTPTS,` +
          `volume=${gain},adelay=${delayMs}|${delayMs}[a${k}]`
      );
    } else {
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

function splitTimeline(timeline) {
  const all = normClips(timeline);
  const videoClips = all
    .filter((c) => (c.kind || "video") === "video")
    .map((c) => ({ ...c, kind: "video", track: 0 }));
  const audioClips = all
    .filter((c) => c.kind === "audio")
    .map((c) => ({ ...c, kind: "audio" }));
  return { videoClips, audioClips };
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

// Dedicated uploader for ASSETS bucket (fixes “wrong endpoint” when assets is in another region)
function makeAssetsS3Client() {
  const region =
    process.env.S3_ASSETS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;

  if (!region) {
    throw new Error("Missing env S3_ASSETS_REGION (or AWS_REGION) for assets bucket uploads");
  }

  // If you use a custom endpoint (rare), set S3_ASSETS_ENDPOINT.
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

export function registerGeneratePublish(app, deps = {}) {
  const { pool, requireAuth, uploadFileToS3 } = deps;

  if (!pool) throw new Error("registerGeneratePublish: missing pool");
  if (!requireAuth) throw new Error("registerGeneratePublish: missing requireAuth");
  if (!uploadFileToS3) throw new Error("registerGeneratePublish: missing uploadFileToS3");

  app.post("/api/generate/publish", requireAuth, async (req, res) => {
    // ✅ req exists here
    console.log("[publish] HIT", {
      origin: req.headers.origin,
      userId: req.user?.id,
      bodyKeys: Object.keys(req.body || {}),
    });

    try {
      const userId = Number(req.user?.id);
      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "mytube-export-"));
      const inputDir = path.join(tmpRoot, "inputs");
      const outDir = path.join(tmpRoot, "out");
      fs.mkdirSync(inputDir, { recursive: true });
      fs.mkdirSync(outDir, { recursive: true });

      const cleanup = () => {
        try {
          fs.rmSync(tmpRoot, { recursive: true, force: true });
        } catch {}
      };

      try {
        const {
          title,
          description = "",
          tags = "",
          visibility = "public",
          timelineName = "Timeline",

          timeline,
          videoClips,
          audioClips,
        } = req.body || {};

        let vClips = [];
        let aClips = [];

        if (Array.isArray(timeline)) {
          const split = splitTimeline(timeline);
          vClips = split.videoClips;
          aClips = split.audioClips;
        } else {
          vClips = normClips(videoClips, { kindDefault: "video" }).map((c) => ({
            ...c,
            kind: "video",
            track: 0,
          }));
          aClips = normClips(audioClips, { kindDefault: "audio" }).map((c) => ({
            ...c,
            kind: "audio",
          }));
        }

        if (!String(title || "").trim()) {
          cleanup();
          return res.status(400).json({ error: "Title is required" });
        }
        if (!vClips.length) {
          cleanup();
          return res.status(400).json({ error: "Video lane is empty" });
        }

        const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
        if (!uploadsBucket) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_UPLOADS_BUCKET" });
        }

        const assetsBucket = process.env.S3_ASSETS_BUCKET;
        if (!assetsBucket) {
          cleanup();
          return res.status(500).json({ error: "Missing env S3_ASSETS_BUCKET" });
        }

        // unique sources across video + audio
        const allIds = Array.from(new Set([...vClips, ...aClips].map((c) => String(c.videoId))));

        const q = await pool.query(
          `SELECT id, filename FROM videos WHERE id::text = ANY($1::text[])`,
          [allIds]
        );

        const byId = new Map(q.rows.map((r) => [String(r.id), String(r.filename || "")]));

        for (const id of allIds) {
          const key = byId.get(id);
          if (!key) {
            cleanup();
            return res.status(400).json({ error: `Unknown/missing source for videoId ${id}` });
          }
        }

        // download each unique source once
        const inputPaths = [];
        const idToInputIndex = new Map();

        for (let i = 0; i < allIds.length; i++) {
          const id = allIds[i];
          const key = byId.get(id);

          if (key.endsWith("/master.m3u8")) {
            const prefix = key.replace(/master\.m3u8$/i, "");
            const localHlsDir = path.join(inputDir, `hls-${i}-${id}`);
            await downloadS3PrefixToDir({ bucket: uploadsBucket, prefix, outDir: localHlsDir });

            const localMaster = path.join(localHlsDir, "master.m3u8");
            if (!fs.existsSync(localMaster)) {
              cleanup();
              return res.status(500).json({ error: `Downloaded HLS missing master.m3u8 for ${id}` });
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

        // which inputs have audio streams?
        const audioPresentByInputIndex = new Map();
        for (let i = 0; i < inputPaths.length; i++) {
          audioPresentByInputIndex.set(i, await hasAudioStream(inputPaths[i]));
        }

        const totalDur = timelineDurationSeconds(vClips);

        const filter = buildFilterVideoAndAudio({
          videoClips: vClips,
          audioClips: aClips,
          idToInputIndex,
          audioPresentByInputIndex,
          totalDur,
        });

        // 1) Render MP4 intermediate
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

        // 2) Create HLS VOD from MP4
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

        // 3) Extract thumbnail from MP4 (middle frame)
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

        // 4) Upload HLS folder to UPLOADS bucket
        const hlsKeyPrefix = `hls/${userId}/${hlsBase}`;
        await uploadDirToS3({
          uploadFileToS3,
          bucket: uploadsBucket,
          localDir: hlsLocalDir,
          keyPrefix: hlsKeyPrefix,
        });

        // 5) Upload thumb to ASSETS bucket (separate client/region)
        const thumbKey = `thumbs/${userId}/${thumbName}`;

        const assetsS3 = makeAssetsS3Client();
        await uploadFileToAssetsBucket({
          assetsS3,
          bucket: assetsBucket,
          key: thumbKey,
          filePath: thumbPath,
          contentType: "image/jpeg",
        });

        // 6) Insert DB row (filename points to HLS master)
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

        cleanup();
        return res.json({ ok: true, videoId: ins.rows[0].id, timelineName });
      } catch (e) {
        console.error("POST /api/generate/publish error:", e);
        cleanup();
        return res.status(500).json({ error: e?.message || "Failed to publish generated video" });
      }
    } catch (e) {
      console.error("[publish] FAIL", e?.stack || e);
      return res.status(500).json({ error: e?.message || "Publish failed" });
    }
  });
}