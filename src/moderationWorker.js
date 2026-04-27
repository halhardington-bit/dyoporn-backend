import OpenAI from "openai";
import { pool } from "./db.js";

const POLL_INTERVAL = Number(process.env.MODERATION_POLL_INTERVAL || 5000);
const MODEL = process.env.MODERATION_LLM_MODEL || "gpt-5.4";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const REPORT_MEDIUM = String(process.env.MODERATION_REPORT_MEDIUM ?? "true") === "true";

function severityRank(severity) {
  const s = String(severity || "none").toLowerCase();
  return {
    none: 0,
    low: 1,
    medium: 2,
    high: 3,
    extreme: 4,
    critical: 4,
  }[s] ?? 0;
}

function normalizeModerationResult(result) {
  const categories = Array.isArray(result.categories) ? result.categories : [];

  let severity = String(result.severity || "none").toLowerCase();

  const copyrightLike = categories.some((c) =>
    [
      "copyright",
      "trademark",
      "brand_or_logo",
      "existing_character",
      "existing_franchise",
      "music_or_artist",
    ].includes(c)
  );

  if (copyrightLike && severityRank(severity) < severityRank("high")) {
    severity = "high";
  }

  return {
    ...result,
    severity,
  };
}

async function createAutoModerationReport({ video, result, autoAction }) {
  const offense = `Automated moderation: ${result.severity}`;

  const comments = [
    `Auto action: ${autoAction}`,
    `Recommended action: ${result.recommended_action || "unknown"}`,
    `Categories: ${(result.categories || []).join(", ") || "none"}`,
    `Reason: ${result.reason || "No reason provided."}`,
    `Flagged terms: ${(result.flagged_terms || []).join(", ") || "none"}`,
  ].join("\n\n");

  await pool.query(
    `
    INSERT INTO video_reports (
      video_id,
      reporter_id,
      offense,
      comments,
      source,
      severity,
      auto_action,
      subject_user_id,
      video_title_snapshot,
      moderation_result
    )
    VALUES ($1, NULL, $2, $3, 'auto_moderation', $4, $5, $6, $7, $8::jsonb)
    `,
    [
      autoAction === "deleted" ? null : String(video.id),
      offense,
      comments,
      result.severity,
      autoAction,
      video.user_id || null,
      video.title || null,
      JSON.stringify(result),
    ]
  );
}

async function shadowBanVideo(videoId) {
  await pool.query(
    `
    UPDATE videos
    SET visibility = 'private',
        updated_at = NOW()
    WHERE id::text = $1::text
    `,
    [String(videoId)]
  );
}

async function deleteVideoRecord(videoId) {
  await pool.query(
    `
    DELETE FROM videos
    WHERE id::text = $1::text
    `,
    [String(videoId)]
  );
}

async function applyModerationActions(video, rawResult) {
  const result = normalizeModerationResult(rawResult);
  const rank = severityRank(result.severity);

  if (rank >= severityRank("extreme")) {
    await createAutoModerationReport({
      video,
      result,
      autoAction: "deleted",
    });

    await deleteVideoRecord(video.id);
    return result;
  }

  if (rank >= severityRank("high")) {
    await shadowBanVideo(video.id);

    await createAutoModerationReport({
      video,
      result,
      autoAction: "shadow_banned",
    });

    return result;
  }

  if (rank >= severityRank("medium") && REPORT_MEDIUM) {
    await createAutoModerationReport({
      video,
      result,
      autoAction: "reported_only",
    });
  }

  return result;
}

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
    RETURNING id, user_id, title, description, tags, creation_data;
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

IMPORTANT CONTEXT:
This platform allows and expects NSFW, adult, and pornographic content.
Sexual content, nudity, fetishes, and explicit material are NOT violations.

Do NOT flag or penalize content for being sexual.

---

You are reviewing creator-provided generation metadata stored as creation_data.

Your job is ONLY to detect the following violations:

1. Copyright or trademark infringement
   - Recreating existing characters, franchises, brands, studios, logos, songs, or artists
   - Direct references to known IP (movies, games, anime, etc.)

2. Real people usage
   - Any real person, celebrity, influencer, politician, or identifiable individual
   - Includes naming, likeness, or clear imitation

3. Impersonation or likeness misuse
   - “Looks like [real person]”
   - “Inspired by [celebrity]” when clearly targeting a real individual

4. Illegal or platform-breaking instructions
   - ONLY if clearly present in metadata
   - Do NOT assume or infer

---

DO NOT FLAG OR PENALIZE:

- Nudity or explicit sexual content
- Pornographic themes
- Adult characters (18+)
- Revealing clothing or body descriptions
- Sexualized prompts or fetishes
- Fictional characters NOT tied to known IP
- Generic roles like “model”, “actress”, “influencer”
- The words “young adult”, “youthful”, or “early twenties”

These are NORMAL and allowed on this platform.

---

CRITICAL SEXUAL CONTENT RULES:

Sexual content MUST NOT affect severity by itself.

Only escalate severity if sexual content involves:

HIGH severity:
- Real people (celebrity or identifiable individuals)
- Copyrighted or trademarked characters or franchises

EXTREME severity:
- Clear indication of minors or underage individuals in a sexual context
- Explicitly illegal sexual scenarios

IMPORTANT:
Do NOT classify "young adult", "youthful", or similar terms as underage.
Only classify as underage if there is CLEAR evidence of a minor.

---

SEVERITY RULES:

- NONE:
  No violations detected

- LOW:
  Extremely minor or irrelevant signals

- MEDIUM:
  Use ONLY if genuinely ambiguous and requires human review
- Clear copyright/trademark/IP infringement

- HIGH:
  Clear use of a real person
  OR clear impersonation

- EXTREME:
  Only use if:
  - clear underage/minor involvement in sexual context
  - explicit illegal content requiring immediate removal

---

ADDITIONAL RULES:

If categories include ANY of:
copyright, trademark, brand_or_logo, existing_character, existing_franchise, music_or_artist

→ severity MUST be at least "high"

If categories include:
real_person, celebrity_or_public_figure, impersonation

→ severity MUST be at least "high"

If underage/minor sexual content is detected:
→ severity MUST be "extreme"

Try and detect if there's intent in the prompt to generate content of minors. 

---

BEHAVIOR:

- Be precise, not over-sensitive
- Be extra cautious of potential content featuring minors
- Only judge based on provided metadata
- Default to "none" unless there is clear evidence of a violation

---

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
              enum: ["none", "low", "medium", "high", "extreme"],
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
    const rawResult = await moderateCreationData(video);
    const result = await applyModerationActions(video, rawResult);

    if (severityRank(result.severity) < severityRank("extreme")) {
    await saveModerationResult(video.id, result);
    }

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