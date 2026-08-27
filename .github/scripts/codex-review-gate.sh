#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${REPO:?REPO is required}"
: "${PR_NUMBER:?PR_NUMBER is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"

CODEX_LOGIN="${CODEX_LOGIN:-chatgpt-codex-connector}"
HEAD_REPO="${HEAD_REPO:-$REPO}"
TIMEOUT_SECONDS="${CODEX_REVIEW_TIMEOUT_SECONDS:-1800}"
POLL_SECONDS="${CODEX_REVIEW_POLL_SECONDS:-15}"
SHORT_SHA="${HEAD_SHA:0:10}"
MARKER="<!-- codex-review-gate:${HEAD_SHA} -->"
COMMENT_ID=""

has_matching_review() {
  local reviews
  reviews="$(gh api "repos/${REPO}/pulls/${PR_NUMBER}/reviews?per_page=100")"
  jq -e \
    --arg login "$CODEX_LOGIN" \
    --arg sha "$SHORT_SHA" \
    'any(.[]; .user.login == $login and ((.body // "") | contains($sha)))' \
    <<<"$reviews" >/dev/null
}

has_pr_clean_reaction() {
  local reactions
  reactions="$(gh api \
    -H "Accept: application/vnd.github+json" \
    "repos/${REPO}/issues/${PR_NUMBER}/reactions?per_page=100")"
  jq -e \
    --arg login "$CODEX_LOGIN" \
    --arg since "$HEAD_COMMIT_AT" \
    'any(.[]; .user.login == $login and .content == "+1" and .created_at >= $since)' \
    <<<"$reactions" >/dev/null
}

has_trigger_clean_reaction() {
  [[ -n "$COMMENT_ID" ]] || return 1
  local reactions
  reactions="$(gh api \
    -H "Accept: application/vnd.github+json" \
    "repos/${REPO}/issues/comments/${COMMENT_ID}/reactions?per_page=100")"
  jq -e \
    --arg login "$CODEX_LOGIN" \
    'any(.[]; .user.login == $login and .content == "+1")' \
    <<<"$reactions" >/dev/null
}

HEAD_COMMIT_AT="$(
  gh api "repos/${REPO}/commits/${HEAD_SHA}" \
    --jq '.commit.committer.date // .commit.author.date'
)"

if has_matching_review || has_pr_clean_reaction; then
  echo "Codex already reviewed current HEAD ${SHORT_SHA}."
  exit 0
fi

comments="$(gh api "repos/${REPO}/issues/${PR_NUMBER}/comments?per_page=100")"
COMMENT_ID="$(
  jq -r --arg marker "$MARKER" \
    '[.[] | select((.body // "") | contains($marker))] | last | .id // empty' \
    <<<"$comments"
)"

if [[ -z "$COMMENT_ID" && "$HEAD_REPO" == "$REPO" ]]; then
  body="$(printf '@codex review\n\nAutomated merge gate for `%s`.\n%s\n' "$SHORT_SHA" "$MARKER")"
  response="$(
    gh api --method POST \
      "repos/${REPO}/issues/${PR_NUMBER}/comments" \
      -f body="$body"
  )"
  COMMENT_ID="$(jq -r '.id' <<<"$response")"
  echo "Requested Codex review for current HEAD ${SHORT_SHA}."
elif [[ -z "$COMMENT_ID" ]]; then
  echo "External PR detected; waiting for Codex auto-review or a maintainer @codex review request."
else
  echo "Codex review request for current HEAD ${SHORT_SHA} already exists."
fi

if [[ "${CODEX_REVIEW_REQUEST_ONLY:-0}" == "1" ]]; then
  echo "Codex review request phase complete for ${SHORT_SHA}."
  exit 0
fi

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if has_matching_review; then
    echo "Codex review matches current HEAD ${SHORT_SHA}."
    exit 0
  fi
  if has_trigger_clean_reaction; then
    echo "Codex reported no findings for current HEAD ${SHORT_SHA}."
    exit 0
  fi
  if has_pr_clean_reaction; then
    echo "Codex clean-review reaction is newer than current HEAD ${SHORT_SHA}."
    exit 0
  fi
  sleep "$POLL_SECONDS"
done

echo "::error::Codex has not completed a review of current HEAD ${SHORT_SHA}. Re-run the check after Codex responds, or comment @codex review on the PR."
exit 1
