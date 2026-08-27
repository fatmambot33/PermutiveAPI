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

is_codex_actor='(.user.login == $login or .user.login == ($login + "[bot]"))'

has_matching_review() {
  gh api "repos/${REPO}/pulls/${PR_NUMBER}/reviews?per_page=100" | jq -e \
    --arg login "$CODEX_LOGIN" --arg sha "$SHORT_SHA" \
    'any(.[]; (.user.login == $login or .user.login == ($login + "[bot]")) and ((.body // "") | contains($sha)))' >/dev/null
}

has_pr_clean_reaction() {
  gh api -H "Accept: application/vnd.github+json" "repos/${REPO}/issues/${PR_NUMBER}/reactions?per_page=100" | jq -e \
    --arg login "$CODEX_LOGIN" --arg since "$HEAD_COMMIT_AT" \
    'any(.[]; (.user.login == $login or .user.login == ($login + "[bot]")) and .content == "+1" and .created_at >= $since)' >/dev/null
}

find_trigger_comment() {
  COMMENT_ID="$(gh api "repos/${REPO}/issues/${PR_NUMBER}/comments?per_page=100" | jq -r --arg marker "$MARKER" '[.[] | select((.body // "") | contains($marker))] | last | .id // empty')"
}

has_trigger_clean_reaction() {
  [[ -n "$COMMENT_ID" ]] || find_trigger_comment
  [[ -n "$COMMENT_ID" ]] || return 1
  gh api -H "Accept: application/vnd.github+json" "repos/${REPO}/issues/comments/${COMMENT_ID}/reactions?per_page=100" | jq -e \
    --arg login "$CODEX_LOGIN" \
    'any(.[]; (.user.login == $login or .user.login == ($login + "[bot]")) and .content == "+1")' >/dev/null
}

echo "Checking Codex evidence for HEAD ${SHORT_SHA}."
HEAD_COMMIT_AT="$(gh api "repos/${REPO}/commits/${HEAD_SHA}" --jq '.commit.committer.date // .commit.author.date')"

if has_matching_review || has_pr_clean_reaction; then
  echo "Codex already reviewed current HEAD ${SHORT_SHA}."
  exit 0
fi

find_trigger_comment
if [[ "${CODEX_REVIEW_REQUEST_ONLY:-0}" == "1" ]]; then
  if [[ -z "$COMMENT_ID" && "$HEAD_REPO" == "$REPO" ]]; then
    body="$(printf '@codex review\n\nAutomated merge gate for `%s`.\n%s\n' "$SHORT_SHA" "$MARKER")"
    if response="$(gh api --method POST "repos/${REPO}/issues/${PR_NUMBER}/comments" -f body="$body" 2>/dev/null)"; then
      COMMENT_ID="$(jq -r '.id' <<<"$response")"
      echo "Requested Codex review for current HEAD ${SHORT_SHA}."
    else
      echo "::notice::PR workflow token cannot post the Codex request; the dedicated request workflow or a maintainer comment will trigger it."
    fi
  elif [[ -n "$COMMENT_ID" ]]; then
    echo "Codex review request for current HEAD ${SHORT_SHA} already exists."
  else
    echo "External PR detected; waiting for Codex auto-review or a maintainer @codex review request."
  fi
  exit 0
fi

deadline=$((SECONDS + TIMEOUT_SECONDS))
while (( SECONDS < deadline )); do
  if has_matching_review; then echo "Codex review matches current HEAD ${SHORT_SHA}."; exit 0; fi
  if has_trigger_clean_reaction; then echo "Codex reported no findings for current HEAD ${SHORT_SHA}."; exit 0; fi
  if has_pr_clean_reaction; then echo "Codex clean-review reaction is newer than current HEAD ${SHORT_SHA}."; exit 0; fi
  sleep "$POLL_SECONDS"
done

echo "::error::Codex has not completed a review of current HEAD ${SHORT_SHA}. Comment @codex review on the PR, then re-run this check after Codex responds."
exit 1
