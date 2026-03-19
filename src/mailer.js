import crypto from "crypto";
import { Resend } from "resend";
import { pool } from "./db.js";

const resend = new Resend(process.env.RESEND_API_KEY);

export function hashToken(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

export function makeVerificationToken() {
  return crypto.randomBytes(32).toString("hex");
}

function getFrontendBase() {
  const frontendBase = String(process.env.FRONTEND_BASE_URL || "").replace(/\/$/, "");
  if (!frontendBase) {
    throw new Error("Missing FRONTEND_BASE_URL");
  }
  return frontendBase;
}

function getEmailFrom() {
  const from = process.env.EMAIL_FROM;
  if (!from) {
    throw new Error("Missing EMAIL_FROM");
  }
  return from;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildEmailTemplate({
  eyebrow = "DYOP",
  title,
  intro,
  actionLabel,
  actionUrl,
  footer,
  hoursText,
}) {
  const safeEyebrow = escapeHtml(eyebrow);
  const safeTitle = escapeHtml(title);
  const safeIntro = escapeHtml(intro);
  const safeActionLabel = escapeHtml(actionLabel);
  const safeFooter = escapeHtml(footer);
  const safeHoursText = escapeHtml(hoursText);
  const safeActionUrl = escapeHtml(actionUrl);

  return `
<!doctype html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="color-scheme" content="light dark" />
    <meta name="supported-color-schemes" content="light dark" />
    <title>${safeTitle}</title>
  </head>
  <body
    style="margin:0; padding:0; background-color:#0f0f10; color:#ffffff;"
    bgcolor="#0f0f10"
  >
    <div style="display:none; max-height:0; overflow:hidden; opacity:0; mso-hide:all;">
      ${safeTitle}
    </div>

    <table
      role="presentation"
      cellpadding="0"
      cellspacing="0"
      border="0"
      width="100%"
      style="width:100%; border-collapse:collapse; background-color:#0f0f10; margin:0; padding:0;"
      bgcolor="#0f0f10"
    >
      <tr>
        <td align="center" style="padding:32px 16px;" bgcolor="#0f0f10">

          <table
            role="presentation"
            cellpadding="0"
            cellspacing="0"
            border="0"
            width="100%"
            style="max-width:620px; width:100%; border-collapse:collapse;"
          >
            <tr>
              <td align="center">

                <table
                  role="presentation"
                  cellpadding="0"
                  cellspacing="0"
                  border="0"
                  width="100%"
                  style="
                    max-width:500px;
                    width:100%;
                    border-collapse:separate;
                    background-color:#171717;
                    border:1px solid #2a2a2a;
                    border-radius:18px;
                  "
                  bgcolor="#171717"
                >
                  <tr>
                    <td
                      align="center"
                      style="
                        padding:40px 28px 34px;
                        font-family:Arial, Helvetica, sans-serif;
                        color:#ffffff;
                        text-align:center;
                      "
                    >
                      <div
                        style="
                          font-size:20px;
                          line-height:1.2;
                          font-weight:800;
                          letter-spacing:0.05em;
                          color:#ffffff;
                          margin:0 0 18px;
                        "
                      >
                        ${safeEyebrow}
                      </div>

                      <h1
                        style="
                          margin:0 0 12px;
                          font-size:24px;
                          line-height:1.2;
                          font-weight:800;
                          color:#ffffff;
                        "
                      >
                        ${safeTitle}
                      </h1>

                      <p
                        style="
                          margin:0 0 24px;
                          font-size:15px;
                          line-height:1.6;
                          color:#cfcfcf;
                        "
                      >
                        ${safeIntro}
                      </p>

                      <table
                        role="presentation"
                        cellpadding="0"
                        cellspacing="0"
                        border="0"
                        style="margin:0 auto;"
                      >
                        <tr>
                          <td
                            align="center"
                            bgcolor="#2f2f2f"
                            style="
                              border:1px solid #5a5a5a;
                              border-radius:12px;
                            "
                          >
                            <a
                              href="${safeActionUrl}"
                              style="
                                display:inline-block;
                                padding:13px 22px;
                                font-family:Arial, Helvetica, sans-serif;
                                font-size:15px;
                                line-height:1.2;
                                font-weight:700;
                                color:#ffffff;
                                text-decoration:none;
                                background-color:#2f2f2f;
                                border-radius:12px;
                              "
                            >
                              ${safeActionLabel}
                            </a>
                          </td>
                        </tr>
                      </table>

                      <div
                        style="
                          margin-top:24px;
                          padding-top:18px;
                          border-top:1px solid #2a2a2a;
                        "
                      >
                        <p
                          style="
                            margin:0 0 8px;
                            font-size:12px;
                            line-height:1.5;
                            color:#9a9a9a;
                          "
                        >
                          If the button doesn’t work, use this link:
                        </p>

                        <p
                          style="
                            margin:0;
                            font-size:12px;
                            line-height:1.6;
                            color:#b8c7da;
                            word-break:break-word;
                          "
                        >
                          <a
                            href="${safeActionUrl}"
                            style="color:#b8c7da; text-decoration:underline;"
                          >
                            ${safeActionUrl}
                          </a>
                        </p>
                      </div>

                      <p
                        style="
                          margin:18px 0 0;
                          font-size:12px;
                          line-height:1.6;
                          color:#aaaaaa;
                        "
                      >
                        ${safeHoursText}
                      </p>
                    </td>
                  </tr>
                </table>

                <p
                  style="
                    margin:18px auto 0;
                    max-width:500px;
                    font-family:Arial, Helvetica, sans-serif;
                    font-size:12px;
                    line-height:1.6;
                    color:#7a7a7a;
                    text-align:center;
                  "
                >
                  ${safeFooter}
                </p>

              </td>
            </tr>
          </table>

        </td>
      </tr>
    </table>
  </body>
</html>
  `;
}

async function sendEmail({ to, subject, html, text }) {
  const from = getEmailFrom();

  const { data, error } = await resend.emails.send({
    from,
    to: [to],
    subject,
    html,
    text,
  });

  if (error) {
    throw new Error(error.message || "Failed to send email");
  }

  return data;
}

export async function createEmailVerificationToken(userId) {
  const rawToken = makeVerificationToken();
  const tokenHash = hashToken(rawToken);
  const hours = Number(process.env.EMAIL_VERIFY_HOURS || 24);

  await pool.query(
    `
    INSERT INTO email_verification_tokens (user_id, token_hash, expires_at)
    VALUES ($1, $2, now() + ($3 || ' hours')::interval)
    `,
    [userId, tokenHash, hours]
  );

  return rawToken;
}

export async function invalidateUnusedVerificationTokens(userId) {
  await pool.query(
    `
    UPDATE email_verification_tokens
    SET used_at = now()
    WHERE user_id = $1
      AND used_at IS NULL
      AND expires_at > now()
    `,
    [userId]
  );
}

export async function createPasswordResetToken(userId) {
  const rawToken = makeVerificationToken();
  const tokenHash = hashToken(rawToken);
  const hours = Number(process.env.PASSWORD_RESET_HOURS || 2);

  await pool.query(
    `
    INSERT INTO password_reset_tokens (user_id, token_hash, expires_at)
    VALUES ($1, $2, now() + ($3 || ' hours')::interval)
    `,
    [userId, tokenHash, hours]
  );

  return rawToken;
}

export async function invalidateUnusedPasswordResetTokens(userId) {
  await pool.query(
    `
    UPDATE password_reset_tokens
    SET used_at = now()
    WHERE user_id = $1
      AND used_at IS NULL
      AND expires_at > now()
    `,
    [userId]
  );
}

export async function sendVerificationEmail({ email, username, rawToken }) {
  const frontendBase = getFrontendBase();
  const verifyUrl = `${frontendBase}/verify-email?token=${encodeURIComponent(rawToken)}`;
  const hours = Number(process.env.EMAIL_VERIFY_HOURS || 24);

  const subject = "Verify your DYOP email";

  const html = buildEmailTemplate({
    eyebrow: "DYOP",
    title: "Verify your email",
    intro: `Hi ${username || "there"}, verify your email address to unlock uploads, comments, ratings, subscriptions, and history.`,
    actionLabel: "Verify Email",
    actionUrl: verifyUrl,
    hoursText: `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    footer: "If you didn’t create an account, you can safely ignore this email.",
  });

  const text = [
    "Verify your DYOP email",
    "",
    `Hi ${username || "there"},`,
    "",
    "Verify your email address using the link below:",
    verifyUrl,
    "",
    `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    "",
    "If you didn’t create an account, you can safely ignore this email.",
  ].join("\n");

  return sendEmail({
    to: email,
    subject,
    html,
    text,
  });
}

export async function sendPasswordResetEmail({ email, username, rawToken }) {
  const frontendBase = getFrontendBase();
  const resetUrl = `${frontendBase}/reset-password?token=${encodeURIComponent(rawToken)}`;
  const hours = Number(process.env.PASSWORD_RESET_HOURS || 2);

  const subject = "Reset your DYOP password";

  const html = buildEmailTemplate({
    eyebrow: "DYOP",
    title: "Reset your password",
    intro: `Hi ${username || "there"}, click below to choose a new password for your account.`,
    actionLabel: "Reset Password",
    actionUrl: resetUrl,
    hoursText: `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    footer: "If you didn’t request a password reset, you can safely ignore this email.",
  });

  const text = [
    "Reset your DYOP password",
    "",
    `Hi ${username || "there"},`,
    "",
    "Use the link below to reset your password:",
    resetUrl,
    "",
    `This link expires in ${hours} hour${hours === 1 ? "" : "s"}.`,
    "",
    "If you didn’t request a password reset, you can safely ignore this email.",
  ].join("\n");

  return sendEmail({
    to: email,
    subject,
    html,
    text,
  });
}