# Branching model

```
main      deployed code                 ← merge from dev when a release is tested
dev       integration / dev testing     ← merge from sandbox when a batch is tested
sandbox   latest local development      ← merge feature branches here first
feature/* new work                      ← branch from dev
```

Rules:

1. Branch features from `dev`: `git switch -c feature/<short-name> dev`.
2. Open a PR into `sandbox`. CI (lint + tests) must pass.
3. Once the batch on `sandbox` is tested, PR `sandbox → dev`.
4. Once `dev` is validated in the dev environment, PR `dev → main`. `main` is what gets deployed.
5. Hotfixes branch from `main`, merge to `main`, then back-merge `main → dev → sandbox`.

Branch protection (run once after the repo exists; requires admin):

```bash
for b in main dev sandbox; do
  gh api -X PUT "repos/{owner}/{repo}/branches/$b/protection" \
    -f required_status_checks[strict]=true \
    -f required_status_checks[contexts][]=ci \
    -F enforce_admins=false \
    -F required_pull_request_reviews[required_approving_review_count]=0 \
    -F restrictions=null
done
```
