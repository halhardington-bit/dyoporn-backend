import fs from "fs";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

import { pool } from "./db.js";
import { processPublishJob } from "./EndpointPublish.js";

const POLL_INTERVAL = 3000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

const uploadsS3 = makeUploadsS3Client();

async function uploadFileToS3({ bucket, key, filePath, contentType }) {
  const body = fs.createReadStream(filePath);

  await uploadsS3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: contentType || "application/octet-stream",
    })
  );
}

async function claimNextJob() {
  const result = await pool.query(`
    UPDATE publish_jobs
    SET
      status = 'processing',
      progress_stage = 'starting',
      progress_pct = 1,
      started_at = COALESCE(started_at, NOW()),
      finished_at = NULL,
      error_message = NULL
    WHERE id = (
      SELECT id
      FROM publish_jobs
      WHERE status = 'queued'
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING *;
  `);

  return result.rows[0] || null;
}

async function workerLoop() {
  console.log("🚀 Worker started");

  await pool.query("SELECT 1");
  console.log("✅ DB connected");

  while (true) {
    try {
      const job = await claimNextJob();

      if (!job) {
        await sleep(POLL_INTERVAL);
        continue;
      }

      console.log(`🎬 Processing job ${job.id}`);

      await processPublishJob({
        pool,
        uploadFileToS3,
        job,
      });

      console.log(`✅ Finished job ${job.id}`);
    } catch (err) {
      console.error("❌ Worker loop error:", err);
      await sleep(2000);
    }
  }
}

workerLoop().catch((err) => {
  console.error("💥 Fatal worker crash:", err);
  process.exit(1);
});