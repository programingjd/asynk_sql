import com.jfrog.bintray.gradle.BintrayExtension
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
  kotlin("jvm") version "1.2.41"
  `maven-publish`
  id("com.jfrog.bintray") version "1.8.0"
}

group = "info.jdavid.asynk"
version = "0.0.0.1"

repositories {
  jcenter()
}

dependencies {
  compile(kotlin("stdlib-jdk8"))
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:0.22.5")
  testImplementation("junit:junit:4.12")
}

kotlin {
  experimental.coroutines = Coroutines.ENABLE
}

val sourcesJar by tasks.creating(Jar::class) {
  classifier = "sources"
  from(java.sourceSets["main"].allSource)
}

val javadocJar by tasks.creating(Jar::class) {
  classifier = "javadoc"
  from(java.docsDir)
}

tasks.withType(KotlinJvmCompile::class.java).all {
  kotlinOptions {
    jvmTarget = "1.8"
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
  (publications) {
    "mavenJava"(MavenPublication::class) {
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
    name = "${project.group}"
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
}
