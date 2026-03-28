import crypto from "crypto";
import { pool } from "./db.js";

function makeMediaKey() {
  return crypto.randomBytes(32).toString("base64");
}

export async function getOrCreateUserMediaKey(userId) {
  const result = await pool.query(
    `
    SELECT media_key
    FROM users
    WHERE id = $1
    LIMIT 1
    `,
    [userId]
  );

  let mediaKey = result.rows[0]?.media_key || null;

  if (!mediaKey) {
    mediaKey = makeMediaKey();

    await pool.query(
      `
      UPDATE users
      SET media_key = $2
      WHERE id = $1
      `,
      [userId, mediaKey]
    );
  }

  return mediaKey;
}

export function mediaKeyBase64ToBuffer(mediaKey) {
  return Buffer.from(mediaKey, "base64");
}