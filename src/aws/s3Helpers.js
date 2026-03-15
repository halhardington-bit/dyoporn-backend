import fs from "fs";
import path from "path";
import {
  PutObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import { s3 } from "./s3Client.js";

export function contentTypeForKey(key) {
  const ext = path.extname(key).toLowerCase();
  if (ext === ".m3u8") return "application/vnd.apple.mpegurl";
  if (ext === ".ts") return "video/mp2t";
  if (ext === ".mp4") return "video/mp4";
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".vtt") return "text/vtt";
  return "application/octet-stream";
}

// Recursively list ALL files under dirPath
function listFilesRecursive(dirPath) {
  const out = [];

  function walk(curr) {
    const entries = fs.readdirSync(curr, { withFileTypes: true });
    for (const e of entries) {
      const full = path.join(curr, e.name);
      if (e.isDirectory()) walk(full);
      else if (e.isFile()) out.push(full);
    }
  }

  walk(dirPath);
  return out;
}

// Upload everything under dirPath to s3://bucket/keyPrefix/... preserving structure
export async function uploadDirToS3({ bucket, dirPath, keyPrefix }) {
  const files = listFilesRecursive(dirPath);
  const basePrefix = String(keyPrefix || "").replace(/\/$/, "");

  for (const filePath of files) {
    // rel path like "v0/playlist.m3u8" even on Windows
    const rel = path.relative(dirPath, filePath).split(path.sep).join("/");
    const key = `${basePrefix}/${rel}`;

    const isPlaylist = rel.toLowerCase().endsWith(".m3u8");

    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: fs.createReadStream(filePath),
        ContentType: contentTypeForKey(key),
        CacheControl: isPlaylist
          ? "public, max-age=60, must-revalidate"
          : "public, max-age=31536000, immutable",
      })
    );
  }
}

// Upload a single file (used for thumbs / direct uploads)
export async function uploadFileToS3({
  bucket,
  key,
  filePath,
  contentType,
  cacheControl,          // optional override
  contentDisposition,    // optional override
}) {
  const ct = contentType || contentTypeForKey(key);

  // sensible defaults
  const isPlaylist = String(key).toLowerCase().endsWith(".m3u8");
  const isSegment  = String(key).toLowerCase().endsWith(".ts");
  const isThumb    = /\.(jpg|jpeg)$/i.test(String(key));
  const isMp4      = String(key).toLowerCase().endsWith(".mp4");

  const cc =
    cacheControl ??
    (isPlaylist
      ? "no-cache"
      : isMp4 || isSegment || isThumb
      ? "public, max-age=31536000, immutable"
      : "public, max-age=3600");

  // Helps some browsers/players; also makes downloading nicer if someone saves it.
  // For streaming MP4 via <video>, "inline" is what you want.
  const cd =
    contentDisposition ??
    (isMp4 ? `inline; filename="${path.basename(key)}"` : undefined);

  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fs.createReadStream(filePath),
      ContentType: ct,
      CacheControl: cc,
      ...(cd ? { ContentDisposition: cd } : {}),
    })
  );

  return { bucket, key };
}


// Delete a single object
export async function deleteFromS3({ bucket, key }) {
  if (!bucket || !key) return;

  await s3.send(
    new DeleteObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );
}

// Delete everything under a prefix (useful for HLS folders)
export async function deletePrefixFromS3({ bucket, prefix }) {
  if (!bucket || !prefix) return;

  let token = undefined;

  do {
    const listed = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: token,
      })
    );

    const objects = (listed.Contents || []).map((o) => ({ Key: o.Key }));
    if (objects.length) {
      await s3.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: { Objects: objects, Quiet: true },
        })
      );
    }

    token = listed.IsTruncated ? listed.NextContinuationToken : undefined;
  } while (token);
}

export function guessVideoContentType(filename) {
  const ext = path.extname(String(filename || "")).toLowerCase();
  if (ext === ".mp4") return "video/mp4";
  if (ext === ".webm") return "video/webm";
  return "application/octet-stream";
}
