import crypto from "crypto";
import bcrypt from "bcryptjs";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
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

const uploadsS3 = makeUploadsS3Client();
const uploadsBucket = process.env.S3_UPLOADS_BUCKET;

function chunk(arr, size = 1000) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function deleteS3Keys(keys) {
  const clean = [...new Set(keys.filter(Boolean))];
  if (!clean.length || !uploadsBucket) return;

  for (const batch of chunk(clean, 1000)) {
    await uploadsS3.send(
      new DeleteObjectsCommand({
        Bucket: uploadsBucket,
        Delete: {
          Objects: batch.map((Key) => ({ Key })),
          Quiet: true,
        },
      })
    );
  }
}

function deriveExtraVideoKeys(video) {
  const keys = [];

  if (video.filename) keys.push(video.filename);
  if (video.thumb) keys.push(video.thumb);

  if (video.hls_master_key) keys.push(video.hls_master_key);

  if (video.hls_prefix) {
    // if you stored a prefix, ideally list+delete that prefix elsewhere
    // simple direct deletion only works for exact keys already known
  }

  return keys;
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

    // optional: invalidate older unused tokens first
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
      SELECT t.id, t.user_id, t.expires_at, t.used_at, u.password_hash, u.email
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
      SELECT id, filename, thumb, hls_master_key, hls_prefix
      FROM videos
      WHERE user_id = $1
      `,
      [row.user_id]
    );

    const s3Keys = videosRes.rows.flatMap(deriveExtraVideoKeys);

    // Delete related data first
    await client.query(`DELETE FROM comment_likes WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM comments WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM ratings WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM reports WHERE reporter_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM watch_history WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM watch_later WHERE user_id = $1`, [row.user_id]);
    await client.query(`DELETE FROM beta_signups WHERE user_id = $1`, [row.user_id]);

    // Delete user-owned videos and related rows
    await client.query(`DELETE FROM video_reports WHERE video_id IN (SELECT id FROM videos WHERE user_id = $1)`, [row.user_id]);
    await client.query(`DELETE FROM comments WHERE video_id IN (SELECT id FROM videos WHERE user_id = $1)`, [row.user_id]);
    await client.query(`DELETE FROM ratings WHERE video_id IN (SELECT id FROM videos WHERE user_id = $1)`, [row.user_id]);
    await client.query(`DELETE FROM watch_history WHERE video_id IN (SELECT id FROM videos WHERE user_id = $1)`, [row.user_id]);
    await client.query(`DELETE FROM watch_later WHERE video_id IN (SELECT id FROM videos WHERE user_id = $1)`, [row.user_id]);
    await client.query(`DELETE FROM videos WHERE user_id = $1`, [row.user_id]);

    await client.query(
      `UPDATE account_delete_tokens SET used_at = NOW() WHERE id = $1`,
      [row.id]
    );

    await client.query(`DELETE FROM users WHERE id = $1`, [row.user_id]);

    await client.query("COMMIT");

    // delete storage after commit
    try {
      await deleteS3Keys(s3Keys);
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