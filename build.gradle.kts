import java.util.Properties

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
}

// ↓↓↓ 追加: local.properties からAPIキーを読み込む ↓↓↓
val localProperties = Properties()
val localPropertiesFile = rootProject.file("local.properties")
if (localPropertiesFile.exists()) {
    localPropertiesFile.inputStream().use { stream -> localProperties.load(stream) }
}
// ↑↑↑ 追加ここまで ↑↑↑

android {
    namespace = "com.sato.oshicam"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.sato.oshicam"
        minSdk = 24
        targetSdk = 36
        versionCode = 4
        versionName = "1.03"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        // ↓↓↓ 追加: APIキーをコードから参照できるようにする ↓↓↓
        buildConfigField("String", "REVENUECAT_API_KEY", "\"${localProperties["REVENUECAT_API_KEY"]}\"")
        buildConfigField("String", "ADMOB_REWARDED_ID", "\"${localProperties["ADMOB_REWARDED_ID"]}\"")
        buildConfigField("String", "ADMOB_INTERSTITIAL_ID", "\"${localProperties["ADMOB_INTERSTITIAL_ID"]}\"")
        // ↑↑↑ 追加ここまで ↑↑↑
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    buildFeatures {
        compose = true
        buildConfig = true  // ↓ 追加: BuildConfig を有効にする
    }
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)

    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)

    // ML Kit (物体追跡・顔認識)
    implementation("com.google.mlkit:object-detection:17.0.2")
    implementation("com.google.mlkit:face-detection:16.1.6")

    // FFmpeg
    implementation("com.antonkarpenko:ffmpeg-kit-full-gpl:2.1.0")

    // 課金管理
    implementation("com.revenuecat.purchases:purchases:10.1.0")

    // 広告
    implementation("com.google.android.gms:play-services-ads:23.0.0")
}
