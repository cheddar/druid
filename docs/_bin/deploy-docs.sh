#!/bin/bash -e

opt_api=1
opt_docs=1
while getopts ":adn" opt; do
  case $opt in
    n)
      opt_dryrun="1"
      ;;
    d)
      opt_api=
      ;;
    a)
      opt_docs=
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done
shift $((OPTIND-1))

if [ -z "$1" ]; then 
    version="latest"
else
    version=$1
fi

druid=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)

if [ -n "$(git -C "$druid" status --porcelain --untracked-files=no)" ]; then
  echo "Working directory is not clean, aborting"
  exit 1
fi

branch=druid-$version
if [ "$version" == "latest" ]; then
  branch=0.6.x
fi

if [ -z "$(git tag -l "$branch")" ] && [ "$branch" != "0.6.x" ]; then
  echo "Version tag does not exist: druid-$version"
  exit 1;
fi

tmp=$(mktemp -d -t druid-docs-deploy)
target=$tmp/docs
src=$tmp/druid

echo "Using Version     [$version]"
echo "Working directory [$tmp]"

git clone -q --depth 1 git@github.com:druid-io/druid-io.github.io.git "$target"

remote=$(git -C "$druid" config --local --get remote.origin.url)
git clone -q --depth 1 --branch $branch $remote "$src"

if [ -n "$opt_docs" ] ; then
  mkdir -p $target/docs/$version
  rsync -a --delete "$src/docs/content/" $target/docs/$version
fi

# generate javadocs for releases (only for tagged releases)
if [ "$version" != "latest" ] && [ -n "$opt_api" ] ; then
  (cd $src && mvn javadoc:aggregate)
  mkdir -p $target/api/$version
  rsync -a --delete "$src/target/site/apidocs/" $target/api/$version
fi

updatebranch=update-docs-$version

git -C $target checkout -b $updatebranch
git -C $target add -A .
git -C $target commit -m "Update $version docs"
if [ -z "$opt_dryrun" ]; then
  git -C $target push origin $updatebranch

  if [ -n "$GIT_TOKEN" ]; then
  curl -u "$GIT_TOKEN:x-oauth-basic" -XPOST -d@- \
     https://api.github.com/repos/druid-io/druid-io.github.io/pulls <<EOF
{
  "title" : "Update $version docs",
  "head"  : "$updatebranch",
  "base"  : "master"
}
EOF

  else
    echo "GitHub personal token not provided, not submitting pull request"
    echo "Please go to https://github.com/druid-io/druid-io.github.io and submit a pull request from the \`$updatebranch\` branch"
  fi

  rm -rf $tmp
else
  echo "Not pushing. To see changes run:"
  echo "git -C $target diff HEAD^1"
fi
