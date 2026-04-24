/** @type {import('semantic-release').GlobalConfig} */
module.exports = {
  branches: ['main'],
  plugins: [
    // Reads commit messages and decides the next version bump.
    // feat → minor, fix → patch, feat!/BREAKING CHANGE → major.
    // chore/docs/refactor/test/ci produce no release.
    '@semantic-release/commit-analyzer',

    // Generates the release notes that appear in the GitHub release body
    // and at the top of CHANGELOG.md.
    '@semantic-release/release-notes-generator',

    // Keeps CHANGELOG.md up to date in the repo.
    ['@semantic-release/changelog', { changelogFile: 'CHANGELOG.md' }],

    // Bumps package.json version and publishes to the npm public registry.
    ['@semantic-release/npm', { npmPublish: true }],

    // Creates a GitHub release (tag + release notes) and commits the
    // updated package.json / CHANGELOG.md back to main.
    [
      '@semantic-release/git',
      {
        assets: ['package.json', 'CHANGELOG.md'],
        message: 'chore(release): ${nextRelease.version} [skip ci]\n\n${nextRelease.notes}',
      },
    ],

    // Creates the GitHub release on the tag.
    '@semantic-release/github',
  ],
};
