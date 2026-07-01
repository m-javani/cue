# Changelog

## [0.2.0] - 2026-07-01
### Changed
- Refactored dispatch queue to simple process queue for better performance
- Improved retry timing predictability and bucket management
- Simplified dispatch queue cleanup logic
- Enhanced test reliability

### Fixed
- Fixed dispatch queue cursor management and cleanup logic (was broken in v0.1.0)

### CI/CD
- Integrated Codecov coverage upload
- Updated GitHub Actions to latest versions
- Migrated golangci-lint config to v2
- Upgraded to Node.js 24 compatible actions
- Added CI workflow badges

### Chores
- Fixed linting issues and formatted code

## [0.1.0] - 2026-06-28
### Added
- Initial release