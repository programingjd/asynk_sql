import com.jfrog.bintray.gradle.BintrayExtension
import org.jetbrains.dokka.gradle.DokkaTask
import java.io.FileInputStream
import java.io.FileWriter
import org.jetbrains.kotlin.config.CompilerConfiguration
import org.jetbrains.kotlin.gradle.dsl.Coroutines
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmCompile
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
  repositories {
    jcenter()
  }
}

plugins {
  kotlin("jvm") version "1.2.61"
  `maven-publish`
  id("org.jetbrains.dokka") version "0.9.17"
  id("com.jfrog.bintray") version "1.8.1"
}

group = "info.jdavid.asynk"
version = "0.0.0.10"

repositories {
  jcenter()
}

dependencies {
  compile(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.25.3")
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.2.0")
  testRuntime("org.junit.jupiter:junit-jupiter-engine:5.2.0")
}

kotlin {
  experimental.coroutines = Coroutines.ENABLE
}

val dokkaJavadoc by tasks.creating(DokkaTask::class) {
  outputFormat = "javadoc"
  includeNonPublic = false
  skipEmptyPackages = true
  impliedPlatforms = mutableListOf("JVM")
  jdkVersion = 8
  outputDirectory = "${buildDir}/javadoc"
}

val sourcesJar by tasks.creating(Jar::class) {
  classifier = "sources"
  from(sourceSets["main"].allSource)
}

val javadocJar by tasks.creating(Jar::class) {
  classifier = "javadoc"
  from("${buildDir}/javadoc")
  dependsOn("javadoc")
}

tasks.withType(KotlinJvmCompile::class.java).all {
  kotlinOptions {
    jvmTarget = "1.8"
  }
}

tasks.withType<Test> {
  useJUnitPlatform()
  testLogging {
    events("passed", "skipped", "failed")
  }
}

val jar: Jar by tasks
jar.apply {
  manifest {
    attributes["Sealed"] = true
  }
}

publishing {
  repositories {
    maven {
      url = uri("${buildDir}/repo")
    }
  }
  publications {
    register("mavenJava", MavenPublication::class.java) {
      from(components["java"])
      artifact(sourcesJar)
      artifact(javadocJar)
    }
  }
}

bintray {
  user = "programingjd"
  key = {
    "bintrayApiKey".let { key: String ->
      File("local.properties").readLines().findLast {
        it.startsWith("${key}=")
      }?.substring(key.length + 1)
    }
  }()
  //dryRun = true
  publish = true
  setPublications("mavenJava")
  pkg(delegateClosureOf<BintrayExtension.PackageConfig>{
    repo = "maven"
    name = "${project.group}.${project.name}"
    websiteUrl = "https://github.com/programingjd/asynk_sql"
    issueTrackerUrl = "https://github.com/programingjd/asynk_sql/issues"
    vcsUrl = "https://github.com/programingjd/asynk_sql.git"
    githubRepo = "programingjd/asynk_sql"
    githubReleaseNotesFile = "README.md"
    setLicenses("Apache-2.0")
    setLabels("asynk", "sql", "java", "kotlin", "async", "coroutines", "suspend")
    publicDownloadNumbers = true
    version(delegateClosureOf<BintrayExtension.VersionConfig> {
      name = "${project.version}"
      mavenCentralSync(delegateClosureOf<BintrayExtension.MavenCentralSyncConfig> {
        sync = false
      })
    })
  })
}

tasks {
  "test" {
    doLast {
      File("README.md").apply {
        val badge = { label: String, text: String, color: String ->
          "https://img.shields.io/badge/_${label}_-${text}-${color}.png?style=flat"
        }
        readLines().mapIndexed { i, line ->
          when (i) {
            0 -> "![jcenter](${badge("jcenter", "${project.version}", "6688ff")})"
            else -> line
          }
        }.joinToString("\n").let {
          FileWriter(this).apply {
            write(it)
            close()
          }
        }
      }
    }
  }
  "bintrayUpload" {
    dependsOn("check")
  }
  "javadoc" {
    dependsOn("dokkaJavadoc")
  }
}
