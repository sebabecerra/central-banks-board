#!/bin/zsh
set -u

REPO_DIR="/Users/sbc/projects/central-banks-board"
LOG_FILE="$REPO_DIR/scheduled_commit_push.log"
COMMIT_MESSAGE="Scheduled auto commit"

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Starting scheduled commit/push"
  cd "$REPO_DIR" || exit 1

  git add -A

  if git diff --cached --quiet; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] No staged changes to commit"
    exit 0
  fi

  git commit -m "$COMMIT_MESSAGE"
  git push origin main
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] Scheduled commit/push finished"
} >> "$LOG_FILE" 2>&1
