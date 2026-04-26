import OpenAI from "openai";
import { pool } from "./db.js";

const POLL_INTERVAL = Number(process.env.MODERATION_POLL_INTERVAL || 5000);
const MODEL = process.env.MODERATION_LLM_MODEL || "gpt-5.4";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function claimNextVideo() {
  const { rows } = await pool.query(`
    UPDATE videos
    SET moderation_check = TRUE
    WHERE id = (
      SELECT id
      FROM videos
      WHERE moderation_check = FALSE
        AND creation_data IS NOT NULL
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    )
    RETURNING id, title, description, tags, creation_data;
  `);

  return rows[0] || null;
}

async function moderateCreationData(video) {
  const response = await openai.responses.create({
    model: MODEL,
    input: [
      {
        role: "system",
        content: `
You are a content moderation classifier for an AI-generated video platform.

You are reviewing creator-provided generation metadata stored as creation_data.

Your job is to detect:
- possible copyright or trademark infringement
- direct use of real people, celebrities, politicians, public figures, or private individuals
- impersonation or likeness misuse
- explicit requests to recreate existing films, shows, games, characters, brands, studios, logos, songs, artists, or protected franchises
- unsafe or policy-breaking generation instructions

Be conservative but not hysterical.
Do not invent facts.
Only judge based on supplied metadata.

Return JSON only.
        `.trim(),
      },
      {
        role: "user",
        content: JSON.stringify(buildModerationPayload(video), null, 2),
      },
    ],
    text: {
      format: {
        type: "json_schema",
        name: "video_moderation_result",
        strict: true,
        schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            passed: { type: "boolean" },
            severity: {
              type: "string",
              enum: ["none", "low", "medium", "high", "critical"],
            },
            categories: {
              type: "array",
              items: {
                type: "string",
                enum: [
                  "copyright",
                  "trademark",
                  "real_person",
                  "celebrity_or_public_figure",
                  "impersonation",
                  "brand_or_logo",
                  "existing_character",
                  "existing_franchise",
                  "music_or_artist",
                  "unsafe_content",
                  "underage_or_youthful_adult",
                  "sexualized_youthful_character",
                  "other",
                ],
              },
            },
            reason: { type: "string" },
            flagged_terms: {
              type: "array",
              items: { type: "string" },
            },
            recommended_action: {
              type: "string",
              enum: ["allow", "review", "block"],
            },
          },
          required: [
            "passed",
            "severity",
            "categories",
            "reason",
            "flagged_terms",
            "recommended_action",
          ],
        },
      },
    },
  });

  const raw = response.output_text;
  return JSON.parse(raw);
}

function buildModerationPayload(video) {
  const data = video.creation_data || {};
  const dependencies = data.dependencies || {};

  const characters = [];
  const prompts = [];
  const assetNames = new Set();
  const sourceUrls = new Set();
  const loras = new Set();

  for (const [key, item] of Object.entries(dependencies)) {
    if (!item || typeof item !== "object") continue;

    const isCharacter = key.startsWith("characterVault_");

    if (isCharacter) {
      const styling = item.styling || {};
      const identity = item.core_identity || {};

      characters.push({
        key,
        name: identity.character_name || null,
        gender: identity.gender || null,
        age_bracket: identity.age_bracket || null,
        fantasy_race: identity.fantasy_race || null,

        // ✅ trim smaller fields lightly
        clothing_prompt: truncate(styling.clothing_prompt, 300),
        negative_prompt: truncate(styling.negative_prompt, 300),
        custom_prompt: truncate(styling.catch_all_custom, 300),

        // 🔥 trim BIG fields aggressively
        compiled_positive_prompt: truncate(item.compiled_positive_prompt, 800),
        compiled_base_positive_prompt: truncate(item.compiled_base_positive_prompt, 800),
        });

      if (item.compiled_positive_prompt) {
        prompts.push(truncate(item.compiled_positive_prompt, 800));
        }

      if (item.compiled_base_positive_prompt) {
        prompts.push(truncate(item.compiled_base_positive_prompt, 800));
        }

      if (Array.isArray(item.activated_loras)) {
        for (const lora of item.activated_loras) {
          loras.add(String(lora));
        }
      }
    }

    const dna = item.dna || {};
    const editState = dna.edit_state || {};

    const collectClips = (clips) => {
      if (!Array.isArray(clips)) return;

      for (const clip of clips) {
        if (clip?.name) assetNames.add(String(clip.name));
        if (clip?.sourceUrl) sourceUrls.add(String(clip.sourceUrl));
      }
    };

    if (Array.isArray(editState.audioTracks)) {
      for (const track of editState.audioTracks) {
        collectClips(track.clips);
      }
    }

    if (Array.isArray(editState.videoTracks)) {
      for (const track of editState.videoTracks) {
        collectClips(track.clips);
      }
    }
  }

  return {
    video: {
      id: video.id,
      title: video.title || null,
      description: video.description || null,
      tags: video.tags || [],
    },
    pipeline: data.pipeline || null,
    declared_moderation: data.moderation || null,
    characters: characters.slice(0, 3),
    prompts: [...new Set(prompts)].slice(0, 0),
    activated_loras: [...loras],
    asset_names: [...assetNames].slice(0, 300),
    source_urls: [...sourceUrls].slice(0, 300),
    checks_requested: [
      "copyright",
      "trademark",
      "real_person",
      "celebrity_or_public_figure",
      "impersonation",
      "brand_or_logo",
      "existing_character",
      "existing_franchise",
      "music_or_artist",
      "unsafe_content",
      "underage_or_youthful_adult",
      "sexualized_youthful_character",
    ],
  };
}

function truncate(str, max = 500) {
  if (!str) return str;
  return str.length > max ? str.slice(0, max) : str;
}

async function saveModerationResult(videoId, result) {
  await pool.query(
    `
    UPDATE videos
    SET
      moderation_result = $2::jsonb,
      moderation_checked_at = NOW(),
      moderation_severity = $3,
      moderation_reason = $4
    WHERE id = $1;
    `,
    [
      videoId,
      JSON.stringify(result),
      result.severity || null,
      result.reason || null,
    ]
  );
}

async function markAsFailedButChecked(videoId, error) {
  await pool.query(
    `
    UPDATE videos
    SET
      moderation_result = $2::jsonb,
      moderation_checked_at = NOW(),
      moderation_severity = 'medium',
      moderation_reason = 'Moderation check failed and requires manual review.'
    WHERE id = $1;
    `,
    [
      videoId,
      JSON.stringify({
        passed: false,
        severity: "medium",
        categories: ["other"],
        reason: "Moderation check failed and requires manual review.",
        error: String(error?.message || error),
        flagged_terms: [],
        recommended_action: "review",
      }),
    ]
  );
}

async function processOneVideo() {
  const video = await claimNextVideo();

  if (!video) {
    return false;
  }

  console.log(`[moderation] Claimed video ${video.id}`);

  try {
    const result = await moderateCreationData(video);

    await saveModerationResult(video.id, result);

    console.log(
      `[moderation] Completed video ${video.id}: ${result.recommended_action} / ${result.severity}`
    );
  } catch (err) {
    console.error(`[moderation] Failed video ${video.id}`, err);
    await markAsFailedButChecked(video.id, err);
  }

  return true;
}

async function main() {
  console.log("[moderation] Worker started");

  while (true) {
    try {
      const didWork = await processOneVideo();

      if (!didWork) {
        await sleep(POLL_INTERVAL);
      }
    } catch (err) {
      console.error("[moderation] Worker loop error", err);
      await sleep(POLL_INTERVAL);
    }
  }
}

main().catch((err) => {
  console.error("[moderation] Fatal worker error", err);
  process.exit(1);
});