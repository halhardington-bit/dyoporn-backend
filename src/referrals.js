export const REFERRAL_COOKIE_NAME = "dyop_referral";

export function normalizeReferral(value) {
  if (!value) return null;

  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "")
    .slice(0, 80);
}

export function getReferralCookieOptions() {
  const isProd = process.env.NODE_ENV === "production";

  return {
    httpOnly: true,
    sameSite: isProd ? "none" : "lax",
    secure: isProd,
    path: "/",
    maxAge: 1000 * 60 * 60 * 24 * 30,
  };
}

export function encodeReferralCookie(referral) {
  return Buffer.from(JSON.stringify(referral), "utf8").toString("base64url");
}

export function decodeReferralCookie(value) {
  if (!value) return null;

  try {
    return JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    return null;
  }
}

export function getReferralFromRequest(req) {
  return decodeReferralCookie(req.cookies?.[REFERRAL_COOKIE_NAME]);
}