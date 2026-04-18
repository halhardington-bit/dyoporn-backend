import { pool } from "./db.js";

export async function reconcileExpiredPlanForUser(userId) {
  if (!userId) return;

  await pool.query(
    `
    UPDATE users
    SET
      tier = 'Free',
      plan_expiry = NULL
    WHERE id = $1
      AND tier <> 'Free'
      AND plan_active = FALSE
      AND plan_expiry IS NOT NULL
      AND plan_expiry <= now()
    `,
    [userId]
  );
}

export async function reconcileAllExpiredPlans() {
  const result = await pool.query(
    `
    UPDATE users
    SET
      tier = 'Free',
      plan_expiry = NULL
    WHERE tier <> 'Free'
      AND plan_active = FALSE
      AND plan_expiry IS NOT NULL
      AND plan_expiry <= now()
    RETURNING id, username, tier
    `
  );

  return result.rows;
}