# Releasing

Three published modules - `pgmq-core`, `pgmq-spring-boot-autoconfigure` and
`pgmq-spring-boot-starter` - are released together, always with the same version. The samples are
never published.

There are two ways to get them into another project:

- [**A GitHub release through JitPack**](#release-on-github-through-jitpack), for anyone to use.
- [**Locally**](#local-releases-for-testing), to try a change in your own application before
  releasing it.

---

## Before any release

Run the whole suite. Neither JitPack nor a local publish runs tests.

```bash
./gradlew build                                             # everything, on PGMQ 1.13.0
./gradlew test -Dpgmq.image=ghcr.io/pgmq/pg17-pgmq:v1.5.1   # the oldest supported PGMQ
```

Both need Docker.

---

## Release on GitHub through JitPack

[JitPack](https://jitpack.io) builds a release on demand from a git tag and serves it as a Maven
repository. It is free for public repositories; nothing is uploaded and no credentials are
involved.

### One-time setup

1. Push the repository to [GitHub](https://github.com/igorsyrbu/pgmq-spring) as a **public**
   repository.
2. Confirm that `repositoryUrl` in `gradle.properties` is
   `https://github.com/igorsyrbu/pgmq-spring`. It is written into every POM.
3. Confirm that the JitPack badge and coordinates in `README.md` use the `igorsyrbu` account.

`jitpack.yml` already defines the build: JDK 17, then `publishToMavenLocal` with the group and
version JitPack supplies, without tests (JitPack's build machines have no Docker).

### Releasing a version

Use semantic version tags without a `v` prefix, such as `0.2.0`. JitPack uses the tag as the
dependency version.

1. Run the suite ([above](#before-any-release)).
2. Tag the commit and push the tag:

   ```bash
   git tag 0.2.0
   git push origin 0.2.0
   ```

3. Optionally, create a GitHub release for the tag (**Releases → Draft a new release**) to publish
   [release notes](#writing-release-notes). JitPack only needs the tag.
4. Open [the pgmq-spring page on JitPack](https://jitpack.io/#igorsyrbu/pgmq-spring), find the tag
   and click **Get it**. That starts
   the build; the log is linked from the same page. Without this step, the first dependency request
   triggers the build instead, and waits for it.

### Writing release notes

Use GitHub Releases as the changelog. Set the title to the version and a short summary, such as
`0.2.0 — Consumer configuration and performance improvements`.

Review the commits and documentation changes between the previous tag and the release tag
(`git log --oneline 0.1.0..0.2.0`, for example). Describe user-visible changes in short bullets,
using only the sections that apply:

- **Added:** new features and configuration options.
- **Fixed:** corrected behaviour.
- **Breaking changes:** changed defaults or APIs, with concrete migration instructions.
- **Notes:** delivery semantics, requirements or limitations users should know about.
- **Installation:** the dependency coordinate with the release version and a link to the tagged
  README's quickstart.

For later releases, add a **Full changelog** link, for example
`https://github.com/igorsyrbu/pgmq-spring/compare/0.1.0...0.2.0`. For the initial release, summarize
the features and requirements present at its tag instead.

In the GitHub release form, select the release tag and previous tag, paste the notes, then publish
or save a draft. **Generate release notes** mainly summarizes merged pull requests; review and
supplement it for changes pushed directly.

### Using a release

The group is `com.github.igorsyrbu.pgmq-spring`, each module is an artifact, and the version is the
tag.

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.igorsyrbu.pgmq-spring:pgmq-spring-boot-starter:0.2.0'
}
```

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.igorsyrbu.pgmq-spring</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>0.2.0</version>
</dependency>
```

The starter brings `pgmq-core` and `pgmq-spring-boot-autoconfigure` transitively. Depend on
`pgmq-core` alone for the client and container without Spring Boot.

### Things to know

- **A tag is built once.** JitPack caches the result, so moving a tag does not rebuild it. To fix a
  release, tag a new version. A failed build can be deleted from its log page on JitPack (while
  signed in with GitHub) and then retried.
- **Unreleased code** is available too: use a commit hash, or `<branch>-SNAPSHOT` (for example
  `main-SNAPSHOT`), as the version. Snapshots are rebuilt when the branch moves, so they are for
  trying things, not for depending on.
- **Private repositories** need a paid JitPack plan.
- **Preview what JitPack will publish** by running its command yourself into a throwaway
  repository, then inspecting the POMs:

  ```bash
  ./gradlew publishToMavenLocal -Pgroup=com.github.igorsyrbu -Pversion=0.2.0 \
      -Dmaven.repo.local=/tmp/jitpack-preview
  ls /tmp/jitpack-preview/com/github/igorsyrbu/*/0.2.0/
  ```

  `com.github.igorsyrbu` is JitPack's build-time `GROUP`. When JitPack exposes an individual
  module, it inserts the repository name and serves it under `com.github.igorsyrbu.pgmq-spring`.

  Each module should have a jar, a sources jar, a javadoc jar, a `.pom` and a `.module`. Every
  dependency in the POMs has an explicit version, there is no `<dependencyManagement>` section,
  and `pgmq-core`'s POM lists no test dependencies.

---

## Local releases for testing

For trying a change in an application on the same machine, without tagging anything.

### Option 1: publish to the local Maven repository

```bash
./gradlew publishToMavenLocal -Pversion=0.2.0-local
```

This installs the three modules into `~/.m2/repository/io/github/igorsyrbu/pgmq/` under the group from
`gradle.properties`. Use a version of your own (`0.2.0-local` above) so it cannot be confused with a
real release. Leaving out `-Pversion` publishes `0.2.0`.

In the application:

```groovy
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation 'io.github.igorsyrbu.pgmq:pgmq-spring-boot-starter:0.2.0-local'
}
```

Maven reads `~/.m2` automatically, so a Maven application only needs the dependency:

```xml
<dependency>
    <groupId>io.github.igorsyrbu.pgmq</groupId>
    <artifactId>pgmq-spring-boot-starter</artifactId>
    <version>0.2.0-local</version>
</dependency>
```

Re-run the publish after every change. The application picks up the new jars on its next build;
if it does not, refresh dependencies (`./gradlew build --refresh-dependencies`, or
`mvn -U package`).

To remove local releases:

```bash
rm -rf ~/.m2/repository/io/github/igorsyrbu/pgmq
```

### Option 2: build from source with a Gradle composite build

For a Gradle application, point its `settings.gradle` at this checkout:

```groovy
includeBuild('../pgmq-spring')
```

and keep the normal dependency:

```groovy
dependencies {
    implementation 'io.github.igorsyrbu.pgmq:pgmq-spring-boot-starter:0.2.0'
}
```

Gradle replaces the dependency with the projects in the checkout, whatever version is declared, and
rebuilds them as part of the application's build. There is nothing to publish, and every change is
picked up immediately, including in an IDE. The application's Gradle must be 9.x, the same major
version as this build's wrapper. Remove the `includeBuild` line to go back to released versions.

| | `publishToMavenLocal` | Composite build |
|---|---|---|
| Works with Maven applications | yes | no |
| Picks up changes | after re-publishing | immediately |
| Tests exactly what gets released | yes - the same jars and POMs | no - builds from source |
