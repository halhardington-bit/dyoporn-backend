import { pool } from "./db.js";
import {
  processPublishJob,
  fetchJob,
  updateJob,
} from "./EndpointPublish.js";

const POLL_INTERVAL = 3000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function claimNextJob() {
  const result = await pool.query(`
    UPDATE publish_jobs
    SET status = 'processing',
        progress_stage = 'starting',
        progress_pct = 1,
        started_at = NOW()
    WHERE id = (
      SELECT id FROM publish_jobs
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

  // Test DB connection immediately
  await pool.query("SELECT 1");
  console.log("✅ DB connected");

  while (true) {
    try {
      const job = await claimNextJob();

      if (!job) {
        await sleep(POLL_INTERVAL);
        continue;
      }

      console.log("🎬 Processing job", job.id);

      await processPublishJob({
        pool,
        uploadFileToS3: async (...args) => {
          throw new Error("uploadFileToS3 not wired"); // safety
        },
        job,
      });

      console.log("✅ Finished job", job.id);
    } catch (err) {
      console.error("❌ Worker loop error:", err);
      await sleep(2000);
    }
  }
}

// 🔥 CRITICAL: catch startup crashes
workerLoop().catch((err) => {
  console.error("💥 Fatal worker crash:", err);
  process.exit(1);
});