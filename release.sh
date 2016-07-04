# Update version to release version with
# mvn versions:set -DnewVersion=$VERSION
# Update version information in 2 readme.md to new version
mvn clean deploy -P release

# After deploy update version to new staging version
# mvn versions:set -DnewVersion=$VERSION2-STAGING