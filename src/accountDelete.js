import bcrypt from "bcryptjs";
import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { pool } from "./db.js";
import {
  makeDeleteToken,
  hashDeleteToken,
  sendAccountDeleteEmail,
} from "./mailer.js";

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

const uploadsS3 = makeUploadsS3Client();
const assetsS3 = makeAssetsS3Client();

const uploadsBucket = process.env.S3_UPLOADS_BUCKET;
const assetsBucket = process.env.S3_ASSETS_BUCKET;

function chunk(arr, size = 1000) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

async function deleteS3Keys({ client, bucket, keys }) {
  const clean = [...new Set((keys || []).filter(Boolean))];
  if (!bucket || !clean.length) return;

  for (const batch of chunk(clean, 1000)) {
    await client.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: batch.map((Key) => ({ Key })),
          Quiet: true,
        },
      })
    );
  }
}

async function listAllKeysForPrefix({ client, bucket, prefix }) {
  if (!bucket || !prefix) return [];

  const keys = [];
  let continuationToken = undefined;

  do {
    const result = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      })
    );

    for (const item of result.Contents || []) {
      if (item?.Key) keys.push(item.Key);
    }

    continuationToken = result.IsTruncated
      ? result.NextContinuationToken
      : undefined;
  } while (continuationToken);

  return keys;
}

function deriveVideoDeletionTargets(video) {
  const uploadKeys = [];
  const uploadPrefixes = [];
  const assetKeys = [];

  if (video.filename) {
    const filename = String(video.filename);

    if (filename.endsWith("/master.m3u8")) {
      const prefix = filename.replace(/\/master\.m3u8$/i, "");
      uploadPrefixes.push(`${prefix}/`);
    } else {
      uploadKeys.push(filename);
    }
  }

  if (video.thumb && video.thumb !== "placeholder.jpg") {
    assetKeys.push(String(video.thumb));
  }

  return { uploadKeys, uploadPrefixes, assetKeys };
}

export async function requestDeleteEmailForUser(userId) {
  const client = await pool.connect();

  try {
    const userRes = await client.query(
      `
      SELECT id, email, username
      FROM users
      WHERE id = $1
      LIMIT 1
      `,
      [userId]
    );

    const user = userRes.rows[0];
    if (!user) {
      throw new Error("User not found.");
    }

    await client.query(
      `
      DELETE FROM account_delete_tokens
      WHERE user_id = $1
        AND used_at IS NULL
      `,
      [user.id]
    );

    const token = makeDeleteToken();
    const tokenHash = hashDeleteToken(token);
    const minutes = Number(process.env.ACCOUNT_DELETE_MINUTES || 15);
    const expiresAt = new Date(Date.now() + minutes * 60 * 1000);

    await client.query(
      `
      INSERT INTO account_delete_tokens (user_id, token_hash, expires_at)
      VALUES ($1, $2, $3)
      `,
      [user.id, tokenHash, expiresAt]
    );

    await sendAccountDeleteEmail({
      email: user.email,
      username: user.username,
      rawToken: token,
    });
  } finally {
    client.release();
  }
}

export async function confirmDeleteAccount({ token, password }) {
  const tokenHash = hashDeleteToken(token);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tokenRes = await client.query(
      `
      SELECT
        t.id,
        t.user_id,
        t.expires_at,
        t.used_at,
        u.password_hash,
        u.email
      FROM account_delete_tokens t
      JOIN users u ON u.id = t.user_id
      WHERE t.token_hash = $1
      LIMIT 1
      `,
      [tokenHash]
    );

    const row = tokenRes.rows[0];

    if (!row) throw new Error("Invalid delete token.");
    if (row.used_at) throw new Error("This delete link has already been used.");
    if (new Date(row.expires_at).getTime() < Date.now()) {
      throw new Error("This delete link has expired.");
    }

    const ok = await bcrypt.compare(password, row.password_hash);
    if (!ok) throw new Error("Incorrect password.");

    const videosRes = await client.query(
      `
      SELECT id, filename, thumb
      FROM videos
      WHERE user_id = $1
      `,
      [row.user_id]
    );

    const deletionTargets = videosRes.rows.map(deriveVideoDeletionTargets);

    const uploadKeys = deletionTargets.flatMap((x) => x.uploadKeys);
    const uploadPrefixes = deletionTargets.flatMap((x) => x.uploadPrefixes);
    const assetKeys = deletionTargets.flatMap((x) => x.assetKeys);

    await client.query(`DELETE FROM comment_likes WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM video_comments WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM video_ratings WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM video_reports WHERE reporter_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM watch_history WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM watch_later WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM subscriptions WHERE subscriber_id = $1 OR channel_id = $1`, [row.user_id]);

    await client.query(
      `DELETE FROM comment_likes
       WHERE comment_id IN (
         SELECT id FROM video_comments WHERE video_id IN (
           SELECT id FROM videos WHERE user_id = $1
         )
       )`,
      [row.user_id]
    );

    await client.query(
      `DELETE FROM video_comments
       WHERE video_id IN (
         SELECT id FROM videos WHERE user_id = $1
       )`,
      [row.user_id]
    );

    await client.query(
      `DELETE FROM video_ratings
       WHERE video_id IN (
         SELECT id FROM videos WHERE user_id = $1
       )`,
      [row.user_id]
    );

    await client.query(
      `DELETE FROM video_reports
       WHERE video_id IN (
         SELECT id FROM videos WHERE user_id = $1
       )`,
      [row.user_id]
    );

    await client.query(
      `DELETE FROM watch_history
       WHERE video_id IN (
         SELECT id FROM videos WHERE user_id = $1
       )`,
      [row.user_id]
    );

    await client.query(
      `DELETE FROM watch_later
       WHERE video_id IN (
         SELECT id FROM videos WHERE user_id = $1
       )`,
      [row.user_id]
    );

    await client.query(`DELETE FROM videos WHERE user_id = $1`, [row.user_id]);

    await client.query(
      `UPDATE account_delete_tokens
       SET used_at = NOW()
       WHERE id = $1`,
      [row.id]
    );

    await client.query(`DELETE FROM users WHERE id = $1`, [row.user_id]);

    await client.query("COMMIT");

    try {
      const derivedPrefix = `hls/${row.user_id}/`;

      const prefixKeysFromVideos = [];
      for (const prefix of uploadPrefixes) {
        const keys = await listAllKeysForPrefix({
          client: uploadsS3,
          bucket: uploadsBucket,
          prefix,
        });
        prefixKeysFromVideos.push(...keys);
      }

      const allUserHlsKeys = await listAllKeysForPrefix({
        client: uploadsS3,
        bucket: uploadsBucket,
        prefix: derivedPrefix,
      });

      await deleteS3Keys({
        client: uploadsS3,
        bucket: uploadsBucket,
        keys: [...uploadKeys, ...prefixKeysFromVideos, ...allUserHlsKeys],
      });

      await deleteS3Keys({
        client: assetsS3,
        bucket: assetsBucket,
        keys: assetKeys,
      });
    } catch (s3Err) {
      console.error("S3 cleanup failed after account deletion:", s3Err);
    }

    return { success: true };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}