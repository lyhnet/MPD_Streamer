#!/bin/bash

# Exit on error
set -e

# Check if inside a git repo
if [ ! -d .git ]; then
    echo "Error: Not a git repository!"
    exit 1
fi

# Prompt for commit message
read -p "Enter commit message: " COMMIT_MSG

# Ensure message is not empty
if [ -z "$COMMIT_MSG" ]; then
    echo "Commit message cannot be empty!"
    exit 1
fi

# Stage all changes
git add .

# Commit changes
git commit -m "$COMMIT_MSG"

# Get current branch name
BRANCH=$(git branch --show-current)

# Push to GitHub
git push -u origin "$BRANCH"

echo "Changes pushed to GitHub successfully on branch $BRANCH!"
