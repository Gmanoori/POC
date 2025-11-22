## SBT:
- We use % to specify the exact artifact version without any specific scala version(open for all).
- %% automatically resolves the current scala version and gets artifact with that specific suffix to avoid binary incompatibility.
- This is extremely helpful when you want to build your project across multiple Scala versions without hardcoding the Scala version suffix.

**Why do we use `ThisBuild/<version>`?**
- This keyword sets the scalaVersion setting globally for the entire build, i.e., it applies to all subprojects and the root project.
- The / syntax scopes the setting to ThisBuild, meaning all projects in this build inherit this setting.
- This is the idiomatic way to set scalaVersion or version globally in sbt, allowing consistent values and enabling sbt's incremental and task evaluation features.