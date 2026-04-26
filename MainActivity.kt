package com.sato.oshicam // 注意: ご自身のパッケージ名に合わせてください

import android.app.Activity
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.BitmapRegionDecoder
import android.graphics.Rect
import android.media.MediaMetadataRetriever
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.provider.MediaStore
import android.util.Log
import android.util.LruCache
import android.view.WindowManager
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.Canvas
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.gestures.detectTransformGestures
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.interaction.collectIsPressedAsState
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Info
import androidx.compose.material.icons.filled.KeyboardArrowDown
import androidx.compose.material.icons.filled.KeyboardArrowUp
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.clipToBounds
import androidx.compose.ui.draw.scale
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.PathFillType
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalUriHandler
import androidx.compose.ui.platform.LocalView
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextDecoration
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import androidx.core.app.NotificationCompat
import androidx.core.view.WindowCompat
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.antonkarpenko.ffmpegkit.FFmpegKit
import com.google.android.gms.ads.AdError
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.FullScreenContentCallback
import com.google.android.gms.ads.LoadAdError
import com.google.android.gms.ads.MobileAds
import com.google.android.gms.ads.interstitial.InterstitialAd
import com.google.android.gms.ads.interstitial.InterstitialAdLoadCallback
import com.google.android.gms.ads.rewarded.RewardedAd
import com.google.android.gms.ads.rewarded.RewardedAdLoadCallback
import com.google.mlkit.vision.common.InputImage
import com.google.mlkit.vision.face.FaceDetection
import com.google.mlkit.vision.face.FaceDetector
import com.google.mlkit.vision.face.FaceDetectorOptions
import com.google.mlkit.vision.objects.ObjectDetection
import com.google.mlkit.vision.objects.ObjectDetector
import com.google.mlkit.vision.objects.defaults.ObjectDetectorOptions
import com.revenuecat.purchases.PurchaseParams
import com.revenuecat.purchases.Purchases
import com.revenuecat.purchases.PurchasesConfiguration
import com.revenuecat.purchases.getCustomerInfoWith
import com.revenuecat.purchases.getOfferingsWith
import com.revenuecat.purchases.purchaseWith
import com.revenuecat.purchases.restorePurchasesWith
import java.io.File
import java.io.FileOutputStream
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.resume
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt

// --- フォアグラウンドサービス (バックグラウンド処理用) ---
class VideoProcessingService : Service() {
    companion object {
        const val CHANNEL_ID = "video_processing_channel"
        const val NOTIFICATION_ID = 1

        fun start(context: Context, message: String) {
            val intent = Intent(context, VideoProcessingService::class.java).apply {
                action = "START"
                putExtra("message", message)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(intent)
            } else {
                context.startService(intent)
            }
        }

        fun update(context: Context, message: String, progress: Int) {
            val intent = Intent(context, VideoProcessingService::class.java).apply {
                action = "UPDATE"
                putExtra("message", message)
                putExtra("progress", progress)
            }
            context.startService(intent)
        }

        fun stop(context: Context) {
            val intent = Intent(context, VideoProcessingService::class.java).apply {
                action = "STOP"
            }
            context.startService(intent)
        }
    }

    private lateinit var notificationManager: NotificationManager
    private lateinit var builder: NotificationCompat.Builder

    override fun onCreate() {
        super.onCreate()
        notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        createNotificationChannel()

        builder = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("推しカメラ Pro")
            .setContentText("処理を準備中...")
            .setSmallIcon(android.R.drawable.ic_menu_camera)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val action = intent?.action
        val message = intent?.getStringExtra("message") ?: ""
        val progress = intent?.getIntExtra("progress", 0) ?: 0

        when (action) {
            "START" -> {
                builder.setContentText(message).setProgress(100, 0, true)
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    startForeground(NOTIFICATION_ID, builder.build(), ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROCESSING)
                } else {
                    startForeground(NOTIFICATION_ID, builder.build())
                }
            }
            "UPDATE" -> {
                val textWithPercent = "$message ($progress%)"
                builder.setContentText(textWithPercent)
                    .setProgress(100, progress, false)
                    .setStyle(NotificationCompat.BigTextStyle().bigText(textWithPercent))
                notificationManager.notify(NOTIFICATION_ID, builder.build())
            }
            "STOP" -> {
                stopForeground(STOP_FOREGROUND_REMOVE)
                stopSelf()
            }
        }
        return START_NOT_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel(
                CHANNEL_ID,
                "動画処理",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "バックグラウンドでの動画展開や書き出しの進捗を表示します"
            }
            notificationManager.createNotificationChannel(channel)
        }
    }
}

// --- 状態管理 (UiState) ---
enum class AppMode { SELECT_VIDEO, EXTRACTING, LOCK_ON, TRACKING, EDITING, EXPORTING }

data class OshiCamUiState(
    val appMode: AppMode = AppMode.SELECT_VIDEO,
    val processingMessage: String = "",
    val isPlaying: Boolean = false,
    val currentFrameIndex: Int = 0,
    val videoWidth: Float = 1080f,
    val videoHeight: Float = 1920f,
    val isBackgroundExtracting: Boolean = false,
    val bgExtractProgress: Int = 0,
    val estimatedTotalFrames: Int = 0,
    val selectedTrackingPriority: String = "色・柄を優先 (軽量ReID)",
    val selectedRatio: String = "スマホ画面に合わせる (フル)",
    val cropZoomPercent: Float = 100f,
    val selectedTrackAxis: String = "上下左右 (通常)",
    val selectedTrackingMode: String = "常に追従 (滑らか)",
    val selectedFps: Int = 15,
    val selectedExtractionRes: String = "1080p (爆速・軽量)",
    val selectedExtractionQuality: String = "標準 (バランス)",
    val shakeReduction: Float = 80f,
    val manualBoxSizePercent: Float = 15f,
    val boxWidthPercent: Float = 100f,
    val selectedResolution: String = "720p (SNS向け・爆速)",
    val isInterpolationEnabled: Boolean = false,
    val autoTrackOnTap: Boolean = false,
    val isManualBoxAddEnabled: Boolean = false,
    val pauseOnLostTracker: Boolean = true,
    val isTrackingCancelled: Boolean = false,
    val isPremiumVersion: Boolean = false,
    val isSmoothExport: Boolean = true,
    val isBoxEditMode: Boolean = false
)

// --- ViewModel ---
// [修正] bitmapCache をカプセル化し、外部から直接書き換えられないようにした
class OshiCamViewModel : ViewModel() {
    private val maxMemory = (Runtime.getRuntime().maxMemory() / 1024).toInt()
    private val cacheSize = maxMemory / 8

    // [修正] private に変更してアクセサ関数経由でのみ操作可能にした
    private val _bitmapCache = object : LruCache<Int, Bitmap>(cacheSize) {
        override fun sizeOf(key: Int, bitmap: Bitmap): Int {
            return bitmap.byteCount / 1024
        }
    }

    fun getBitmap(key: Int): Bitmap? = _bitmapCache.get(key)
    fun putBitmap(key: Int, bitmap: Bitmap) { _bitmapCache.put(key, bitmap) }
    fun evictAllBitmaps() { _bitmapCache.evictAll() }

    private val _uiState = MutableStateFlow(OshiCamUiState())
    val uiState: StateFlow<OshiCamUiState> = _uiState.asStateFlow()

    fun updateState(update: (OshiCamUiState) -> OshiCamUiState) {
        _uiState.update(update)
    }

    var activeFfmpegSessionId: Long? = null

    var frameFiles = mutableStateListOf<File>()
    val allFramesObjects = mutableStateMapOf<Int, List<Rect>>()
    val trackingMap = mutableStateMapOf<Int, Rect>()
    val userKeyframes = mutableStateListOf<Int>()
    val cutFrames = mutableStateListOf<Int>()
    val interpolatedFrames = mutableStateListOf<Int>()

    fun cancelFfmpegProcess() {
        activeFfmpegSessionId?.let {
            FFmpegKit.cancel(it)
            activeFfmpegSessionId = null
        }
    }

    fun moveCurrentBox(dx: Int, dy: Int, videoWidth: Float, videoHeight: Float) {
        val index = uiState.value.currentFrameIndex
        val box = trackingMap[index] ?: return
        val w = box.width()
        val h = box.height()
        var newLeft = box.left + dx
        var newTop = box.top + dy
        var newRight = box.right + dx
        var newBottom = box.bottom + dy

        if (newLeft < 0) { newLeft = 0; newRight = w }
        if (newRight > videoWidth.toInt()) { newRight = videoWidth.toInt(); newLeft = newRight - w }
        if (newTop < 0) { newTop = 0; newBottom = h }
        if (newBottom > videoHeight.toInt()) { newBottom = videoHeight.toInt(); newTop = newBottom - h }

        trackingMap[index] = Rect(newLeft, newTop, newRight, newBottom)
        if (!userKeyframes.contains(index)) userKeyframes.add(index)
        interpolatedFrames.remove(index)
    }

    override fun onCleared() {
        super.onCleared()
        evictAllBitmaps()
        cancelFfmpegProcess()
    }
}

// --- ユーティリティ関数 ---

fun removeOverlappingBoxes(boxes: List<Rect>): List<Rect> {
    val result = mutableListOf<Rect>()
    for (i in boxes.indices) {
        val box1 = boxes[i]
        var isRedundant = false
        for (j in boxes.indices) {
            if (i == j) continue
            val box2 = boxes[j]
            val overlap = Rect()
            if (overlap.setIntersect(box1, box2)) {
                val area1 = box1.width() * box1.height()
                val area2 = box2.width() * box2.height()
                val overlapArea = overlap.width() * overlap.height()
                if (area1 < area2 && overlapArea > area1 * 0.5f) {
                    isRedundant = true
                    break
                } else if (area1 == area2 && i > j && overlapArea > area1 * 0.8f) {
                    isRedundant = true
                    break
                }
            }
        }
        if (!isRedundant) {
            result.add(box1)
        }
    }
    return result
}

// [修正・新規追加] 重複していた顔・物体検出マージロジックを共通関数に抽出
fun mergeDetectionBoxes(
    rawObjs: List<Rect>,
    rawFaces: List<Rect>,
    scaleFactor: Float,
    imgW: Int,
    imgH: Int,
    boxWidthPercent: Float,
    lastBox: Rect? = null
): List<Rect> {
    val faceBoxes = rawFaces.mapNotNull { f ->
        val fw = f.width() * scaleFactor
        val fh = f.height() * scaleFactor
        if (fw < imgW * 0.005f) return@mapNotNull null
        val fl = f.left * scaleFactor
        val ft = f.top * scaleFactor
        val fr = f.right * scaleFactor
        val fb = f.bottom * scaleFactor
        Rect(
            max(0, (fl - fw * 2.0f).toInt()),
            max(0, (ft - fh * 1.5f).toInt()),
            min(imgW, (fr + fw * 2.0f).toInt()),
            min(imgH, (fb + fh * 4.5f).toInt())
        )
    }

    val objBoxes = rawObjs.map { o ->
        Rect(
            (o.left * scaleFactor).toInt(),
            (o.top * scaleFactor).toInt(),
            (o.right * scaleFactor).toInt(),
            (o.bottom * scaleFactor).toInt()
        )
    }.filter { box ->
        val w = box.width().toFloat()
        val h = box.height().toFloat()
        w > imgW * 0.03f && h > imgH * 0.05f
    }

    val mergedCandidates = mutableListOf<Rect>()
    val usedObjBoxes = mutableSetOf<Rect>()

    for (faceBox in faceBoxes) {
        var bestObj: Rect? = null
        var maxIntersectArea = 0
        for (objBox in objBoxes) {
            val intersect = Rect()
            if (intersect.setIntersect(faceBox, objBox)) {
                val area = intersect.width() * intersect.height()
                if (area > maxIntersectArea) {
                    maxIntersectArea = area
                    bestObj = objBox
                }
            }
        }
        if (bestObj != null) {
            val unionBox = Rect(faceBox)
            unionBox.union(bestObj)
            mergedCandidates.add(unionBox)
            usedObjBoxes.add(bestObj)
        } else {
            mergedCandidates.add(faceBox)
        }
    }

    for (objBox in objBoxes) {
        if (!usedObjBoxes.contains(objBox)) {
            val ratio = objBox.height().toFloat() / objBox.width().toFloat()
            val isTracked = lastBox?.let { Rect.intersects(objBox, it) } ?: false
            if (isTracked || ratio in 0.8f..4.5f) {
                mergedCandidates.add(objBox)
            }
        }
    }

    val narrowedCandidates = mergedCandidates.map { box ->
        if (boxWidthPercent >= 100f) box else {
            val cx = box.centerX()
            val newW = (box.width() * (boxWidthPercent / 100f)).toInt()
            Rect(cx - newW / 2, box.top, cx + newW / 2, box.bottom)
        }
    }

    return removeOverlappingBoxes(narrowedCandidates)
}

// --- 広告管理 (AdMob) ---
// [修正] object シングルトンの mutable 状態を @Volatile で保護し、スレッドセーフ性を向上
object AdManager {
    @Volatile var rewardedAd: RewardedAd? = null
    @Volatile var isRewardedAdLoading = false

    @Volatile var interstitialAd: InterstitialAd? = null
    @Volatile var isInterstitialAdLoading = false

    fun loadRewardedAd(context: Context) {
        if (rewardedAd != null || isRewardedAdLoading) return
        isRewardedAdLoading = true
        val adRequest = AdRequest.Builder().build()
        // [修正] BuildConfig からIDを取得（ハードコード廃止）
        RewardedAd.load(
            context,
            BuildConfig.ADMOB_REWARDED_ID,
            adRequest,
            object : RewardedAdLoadCallback() {
                override fun onAdFailedToLoad(adError: LoadAdError) {
                    rewardedAd = null
                    isRewardedAdLoading = false
                }
                override fun onAdLoaded(ad: RewardedAd) {
                    rewardedAd = ad
                    isRewardedAdLoading = false
                }
            }
        )
    }

    fun loadInterstitialAd(context: Context) {
        if (interstitialAd != null || isInterstitialAdLoading) return
        isInterstitialAdLoading = true
        val adRequest = AdRequest.Builder().build()
        // [修正] BuildConfig からIDを取得（ハードコード廃止）
        InterstitialAd.load(
            context,
            BuildConfig.ADMOB_INTERSTITIAL_ID,
            adRequest,
            object : InterstitialAdLoadCallback() {
                override fun onAdFailedToLoad(adError: LoadAdError) {
                    interstitialAd = null
                    isInterstitialAdLoading = false
                }
                override fun onAdLoaded(ad: InterstitialAd) {
                    interstitialAd = ad
                    isInterstitialAdLoading = false
                }
            }
        )
    }
}

// --- メインアクティビティ ---
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)

        // [修正] RevenueCatキーを BuildConfig から取得（ハードコード廃止）
        try {
            Purchases.debugLogsEnabled = true
            Purchases.configure(
                PurchasesConfiguration.Builder(this, BuildConfig.REVENUECAT_API_KEY).build()
            )
        } catch (e: Exception) {
            Log.e("RevenueCatInit", "RevenueCat初期化エラー: ${e.message}")
        }

        MobileAds.initialize(this) {}
        AdManager.loadRewardedAd(this)
        AdManager.loadInterstitialAd(this)

        val viewModel = ViewModelProvider(this)[OshiCamViewModel::class.java]

        setContent {
            val systemDarkTheme = isSystemInDarkTheme()
            var isDarkTheme by remember { mutableStateOf(systemDarkTheme) }
            val colors = if (isDarkTheme) darkColorScheme() else lightColorScheme()

            val view = LocalView.current
            if (!view.isInEditMode) {
                SideEffect {
                    val window = (view.context as Activity).window
                    window.statusBarColor = if (isDarkTheme) android.graphics.Color.BLACK else android.graphics.Color.WHITE
                    window.navigationBarColor = if (isDarkTheme) android.graphics.Color.BLACK else android.graphics.Color.WHITE
                    WindowCompat.getInsetsController(window, view).isAppearanceLightStatusBars = !isDarkTheme
                    WindowCompat.getInsetsController(window, view).isAppearanceLightNavigationBars = !isDarkTheme
                }
            }

            MaterialTheme(colorScheme = colors) {
                Surface(modifier = Modifier.fillMaxSize().systemBarsPadding(), color = MaterialTheme.colorScheme.background) {
                    OshiCamScreen(
                        isDarkTheme = isDarkTheme,
                        onThemeToggle = { isDarkTheme = !isDarkTheme },
                        viewModel = viewModel
                    )
                }
            }
        }
    }
}

// --- メイン画面 ---
@Composable
fun OshiCamScreen(isDarkTheme: Boolean, onThemeToggle: () -> Unit, viewModel: OshiCamViewModel) {
    val context = LocalContext.current
    val uiState by viewModel.uiState.collectAsState()

    var pendingVideoUri by remember { mutableStateOf<Uri?>(null) }

    var previewScale by remember { mutableFloatStateOf(1f) }
    var offset by remember { mutableStateOf(Offset.Zero) }
    var showCameraPreviewBox by remember { mutableStateOf(true) }
    var previewBitmap by remember { mutableStateOf<Bitmap?>(null) }

    var showHelpDialog by remember { mutableStateOf(false) }
    var showRemoveAfterDialog by remember { mutableStateOf(false) }
    var showFpsDialog by remember { mutableStateOf(false) }
    var showSaveSlotDialog by remember { mutableStateOf(false) }
    var showLoadSlotDialog by remember { mutableStateOf(false) }
    var showDeleteSlotDialog by remember { mutableStateOf(false) }
    var slotRefreshTrigger by remember { mutableIntStateOf(0) }

    val fpsOptions = listOf(15, 30)
    val extractionQualities = listOf("高品質 (重い)", "標準 (バランス)", "低品質 (爆速・軽量)")

    val permissionLauncher = rememberLauncherForActivityResult(ActivityResultContracts.RequestPermission()) {}

    LaunchedEffect(Unit) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            permissionLauncher.launch(android.Manifest.permission.POST_NOTIFICATIONS)
        }

        try {
            Purchases.sharedInstance.getCustomerInfoWith(
                onSuccess = { customerInfo ->
                    viewModel.updateState { it.copy(isPremiumVersion = customerInfo.entitlements["premium"]?.isActive == true) }
                },
                onError = { error -> Log.e("RevenueCat", "課金状況の確認失敗: ${error.message}") }
            )
        } catch (e: Exception) { e.printStackTrace() }

        val originalVideo = File(context.filesDir, "working_video.mp4")
        val framesDir = File(context.filesDir, "extracted_frames")
        if (originalVideo.exists() && framesDir.exists()) {
            val files = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name }
            if (!files.isNullOrEmpty()) {
                viewModel.frameFiles.clear()
                viewModel.frameFiles.addAll(files)
                // [修正] loadProjectData を IO スレッドで実行
                val loadedFps = withContext(Dispatchers.IO) {
                    loadProjectData(context, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                }
                viewModel.updateState { it.copy(selectedFps = loadedFps, appMode = AppMode.EDITING) }
                Toast.makeText(context, "前回の編集データを復元しました", Toast.LENGTH_SHORT).show()
            }
        }
    }

    LaunchedEffect(uiState.isBackgroundExtracting) {
        while (uiState.isBackgroundExtracting && isActive) {
            delay(1000)
            val framesDir = File(context.filesDir, "extracted_frames")
            val files = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name }
            if (files != null && files.isNotEmpty()) {
                viewModel.frameFiles.clear()
                viewModel.frameFiles.addAll(files)
            }
        }
        val framesDir = File(context.filesDir, "extracted_frames")
        val files = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name }
        if (files != null) {
            viewModel.frameFiles.clear()
            viewModel.frameFiles.addAll(files)
        }
    }

    LaunchedEffect(viewModel.frameFiles) {
        if (viewModel.frameFiles.isNotEmpty() && uiState.videoWidth == 1080f) {
            withContext(Dispatchers.IO) {
                try {
                    val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
                    BitmapFactory.decodeFile(viewModel.frameFiles[0].absolutePath, options)
                    viewModel.updateState {
                        it.copy(videoWidth = max(1f, options.outWidth.toFloat()), videoHeight = max(1f, options.outHeight.toFloat()))
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
    }

    LaunchedEffect(uiState.isPlaying) {
        if (uiState.isPlaying) {
            withContext(Dispatchers.IO) {
                val frameDuration = 1000L / uiState.selectedFps
                var nextFrameTime = System.currentTimeMillis() + frameDuration

                while (uiState.currentFrameIndex < viewModel.frameFiles.size - 1 && uiState.isPlaying && isActive) {
                    val nextIdx = uiState.currentFrameIndex + 1

                    // [修正] bitmapCache をアクセサ経由で操作
                    var bmp = viewModel.getBitmap(nextIdx)
                    if (bmp == null) {
                        try {
                            val options = BitmapFactory.Options().apply { inSampleSize = 2; inPreferredConfig = Bitmap.Config.RGB_565 }
                            bmp = BitmapFactory.decodeFile(viewModel.frameFiles[nextIdx].absolutePath, options)
                            if (bmp != null) viewModel.putBitmap(nextIdx, bmp)
                        } catch (e: Exception) { Log.e("Playback", "フレーム読み込みエラー", e) }
                    }

                    if (bmp != null) {
                        withContext(Dispatchers.Main) {
                            previewBitmap = bmp
                            viewModel.updateState { it.copy(currentFrameIndex = nextIdx) }
                        }
                    }

                    val now = System.currentTimeMillis()
                    val sleepTime = nextFrameTime - now
                    if (sleepTime > 0) delay(sleepTime)
                    nextFrameTime += frameDuration
                }
            }
            viewModel.updateState { it.copy(isPlaying = false) }
        }
    }

    LaunchedEffect(uiState.currentFrameIndex, viewModel.frameFiles) {
        if (uiState.appMode == AppMode.TRACKING || uiState.appMode == AppMode.EXPORTING || uiState.isPlaying) return@LaunchedEffect

        if (viewModel.frameFiles.isNotEmpty() && uiState.currentFrameIndex in viewModel.frameFiles.indices) {
            withContext(Dispatchers.IO) {
                // [修正] bitmapCache をアクセサ経由で操作
                var bitmap = viewModel.getBitmap(uiState.currentFrameIndex)
                try {
                    val sampleSize = if (uiState.appMode == AppMode.TRACKING) 2 else 1
                    if (bitmap == null || (sampleSize == 1 && bitmap.width < uiState.videoWidth / 2)) {
                        val options = BitmapFactory.Options().apply { inSampleSize = sampleSize; inPreferredConfig = Bitmap.Config.RGB_565 }
                        bitmap = BitmapFactory.decodeFile(viewModel.frameFiles[uiState.currentFrameIndex].absolutePath, options)
                        if (bitmap != null) viewModel.putBitmap(uiState.currentFrameIndex, bitmap)
                    }
                    if (bitmap != null) {
                        withContext(Dispatchers.Main) { previewBitmap = bitmap }
                    }
                } catch (e: Exception) { Log.e("Preview", "プレビュー読み込みエラー", e) }
            }
        }
    }

    LaunchedEffect(uiState.currentFrameIndex, viewModel.frameFiles, uiState.boxWidthPercent) {
        if (uiState.appMode == AppMode.TRACKING || uiState.appMode == AppMode.EXPORTING || uiState.isPlaying) return@LaunchedEffect

        delay(300)
        if (!isActive) return@LaunchedEffect

        if (viewModel.frameFiles.isNotEmpty() && uiState.currentFrameIndex in viewModel.frameFiles.indices) {
            withContext(Dispatchers.IO) {
                var objDetector: ObjectDetector? = null
                var faceDetector: FaceDetector? = null

                try {
                    val detectOptions = BitmapFactory.Options().apply { inSampleSize = 2; inPreferredConfig = Bitmap.Config.RGB_565 }
                    val detectBitmap = BitmapFactory.decodeFile(viewModel.frameFiles[uiState.currentFrameIndex].absolutePath, detectOptions)

                    if (detectBitmap != null) {
                        val objOptions = ObjectDetectorOptions.Builder().setDetectorMode(ObjectDetectorOptions.SINGLE_IMAGE_MODE).enableMultipleObjects().build()
                        objDetector = ObjectDetection.getClient(objOptions)

                        val faceOptions = FaceDetectorOptions.Builder().setPerformanceMode(FaceDetectorOptions.PERFORMANCE_MODE_FAST).build()
                        faceDetector = FaceDetection.getClient(faceOptions)

                        val image = InputImage.fromBitmap(detectBitmap, 0)
                        val rawObjs = detectObjectsSync(objDetector, image)
                        val rawFaces = detectFacesSync(faceDetector, image)

                        val scaleFactor = 2f
                        val imgW = detectBitmap.width * 2
                        val imgH = detectBitmap.height * 2

                        // [修正] 共通関数 mergeDetectionBoxes を使用（重複コード廃止）
                        val finalFilteredObjs = mergeDetectionBoxes(
                            rawObjs, rawFaces, scaleFactor, imgW, imgH, uiState.boxWidthPercent
                        )
                        detectBitmap.recycle()

                        withContext(Dispatchers.Main) {
                            viewModel.allFramesObjects[uiState.currentFrameIndex] = finalFilteredObjs
                        }
                    }
                } catch (e: Exception) {
                    Log.e("Detection", "検出エラー", e)
                } finally {
                    objDetector?.close()
                    faceDetector?.close()
                }
            }
        }
    }

    val videoPickerLauncher = rememberLauncherForActivityResult(ActivityResultContracts.GetContent()) { uri: Uri? ->
        if (uri != null) {
            pendingVideoUri = uri
            showFpsDialog = true
        }
    }

    if (showFpsDialog && pendingVideoUri != null) {
        AlertDialog(
            onDismissRequest = { showFpsDialog = false; pendingVideoUri = null },
            title = { Text("動画の読み込み設定", fontWeight = FontWeight.Bold) },
            text = {
                Column {
                    Text("1. 動画の滑らかさ (FPS)", style = MaterialTheme.typography.titleSmall)
                    Text("※15fpsを選ぶと、読み込みとAI追従が【爆速】になります。書き出し時は自動で30fpsに滑らか補間されます！", fontSize = 11.sp, color = Color.Gray)
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        fpsOptions.forEach { fps ->
                            Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(selectedFps = fps) } }) {
                                RadioButton(selected = (uiState.selectedFps == fps), onClick = { viewModel.updateState { it.copy(selectedFps = fps) } })
                                Text("${fps}fps", modifier = Modifier.padding(end = 12.dp))
                            }
                        }
                    }
                    Spacer(modifier = Modifier.height(16.dp))
                    Divider()
                    Spacer(modifier = Modifier.height(16.dp))
                    Text("2. 展開画像の品質 (圧縮率)", style = MaterialTheme.typography.titleSmall)
                    extractionQualities.forEach { q ->
                        Row(
                            verticalAlignment = Alignment.CenterVertically,
                            modifier = Modifier.fillMaxWidth().height(40.dp).clickable { viewModel.updateState { it.copy(selectedExtractionQuality = q) } }
                        ) {
                            RadioButton(selected = (uiState.selectedExtractionQuality == q), onClick = { viewModel.updateState { it.copy(selectedExtractionQuality = q) } })
                            Text(q, fontSize = 14.sp)
                        }
                    }
                }
            },
            confirmButton = {
                Button(onClick = {
                    showFpsDialog = false
                    val uri = pendingVideoUri!!
                    pendingVideoUri = null

                    val startExtractionProcess = {
                        previewBitmap = null
                        previewScale = 1f
                        offset = Offset.Zero
                        viewModel.updateState { it.copy(appMode = AppMode.EXTRACTING) }

                        if (uiState.isPremiumVersion) {
                            VideoProcessingService.start(context, "動画を展開中...")
                        } else {
                            Toast.makeText(context, "無料版はアプリを開いたままお待ちください\n(※裏画面に行くと停止する場合があります)", Toast.LENGTH_LONG).show()
                        }

                        viewModel.viewModelScope.launch {
                            try {
                                viewModel.evictAllBitmaps()
                                // [修正] saveProjectData を IO スレッドで実行
                                withContext(Dispatchers.IO) {
                                    val projectFile = File(context.filesDir, "project_data.txt")
                                    if (projectFile.exists()) projectFile.delete()
                                }

                                viewModel.updateState { it.copy(processingMessage = "動画を準備中...(数秒で終わります)") }

                                val retriever = MediaMetadataRetriever()
                                try {
                                    retriever.setDataSource(context, uri)
                                    val durationMs = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION)?.toLongOrNull() ?: 0L
                                    viewModel.updateState { it.copy(estimatedTotalFrames = (durationMs / 1000f * uiState.selectedFps).toInt()) }
                                } finally {
                                    retriever.release()
                                }

                                val copiedFile = copyUriToFile(context, uri)
                                val framesDir = File(context.filesDir, "extracted_frames")
                                if (!framesDir.exists()) framesDir.mkdirs()
                                withContext(Dispatchers.IO) { framesDir.listFiles()?.forEach { it.delete() } }

                                val qValue = when (uiState.selectedExtractionQuality) {
                                    "高品質 (重い)" -> 1; "標準 (バランス)" -> 3; "低品質 (爆速・軽量)" -> 7; else -> 3
                                }
                                val scaleFilter = "-vf \"fps=${uiState.selectedFps},scale='min(1080,iw)':-2\""

                                withContext(Dispatchers.IO) {
                                    val commandFirst = "-y -i '${copiedFile.absolutePath}' -vframes 1 $scaleFilter -q:v $qValue '${framesDir.absolutePath}/frame_00000.jpg'"
                                    FFmpegKit.execute(commandFirst)

                                    val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
                                    BitmapFactory.decodeFile("${framesDir.absolutePath}/frame_00000.jpg", options)
                                    val realW = max(1f, options.outWidth.toFloat())
                                    val realH = max(1f, options.outHeight.toFloat())
                                    withContext(Dispatchers.Main) {
                                        viewModel.updateState { it.copy(videoWidth = realW, videoHeight = realH) }
                                    }
                                }

                                val files = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()
                                viewModel.frameFiles.clear()
                                viewModel.frameFiles.addAll(files)

                                viewModel.trackingMap.clear()
                                viewModel.allFramesObjects.clear()
                                viewModel.userKeyframes.clear()
                                viewModel.cutFrames.clear()
                                viewModel.interpolatedFrames.clear()

                                viewModel.updateState {
                                    it.copy(
                                        currentFrameIndex = 0,
                                        isInterpolationEnabled = false,
                                        autoTrackOnTap = false,
                                        isManualBoxAddEnabled = false,
                                        appMode = AppMode.LOCK_ON,
                                        isBackgroundExtracting = true,
                                        bgExtractProgress = 0
                                    )
                                }

                                // [修正] saveProjectData を IO スレッドで実行
                                withContext(Dispatchers.IO) {
                                    saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                }

                                withContext(Dispatchers.IO) {
                                    val commandFull = "-y -threads 0 -i '${copiedFile.absolutePath}' $scaleFilter -q:v $qValue '${framesDir.absolutePath}/frame_%05d.jpg'"
                                    val session = FFmpegKit.executeAsync(commandFull,
                                        { _ ->
                                            viewModel.updateState { it.copy(isBackgroundExtracting = false) }
                                            viewModel.activeFfmpegSessionId = null
                                            if (uiState.isPremiumVersion) {
                                                VideoProcessingService.stop(context)
                                            }
                                        },
                                        { _ -> },
                                        { statistics ->
                                            if (uiState.estimatedTotalFrames > 0) {
                                                val progress = (statistics.videoFrameNumber.toFloat() / uiState.estimatedTotalFrames * 100).toInt().coerceIn(0, 100)
                                                viewModel.updateState { it.copy(bgExtractProgress = progress) }
                                                if (uiState.isPremiumVersion) {
                                                    VideoProcessingService.update(context, "動画を展開中...", progress)
                                                }
                                            }
                                        }
                                    )
                                    viewModel.activeFfmpegSessionId = session.sessionId
                                }
                            } catch (e: Exception) {
                                Toast.makeText(context, "エラー: ${e.message}", Toast.LENGTH_LONG).show()
                                viewModel.updateState { it.copy(appMode = AppMode.SELECT_VIDEO) }
                                if (uiState.isPremiumVersion) {
                                    VideoProcessingService.stop(context)
                                }
                            }
                        }
                    }

                    if (uiState.isPremiumVersion) {
                        startExtractionProcess()
                    } else {
                        val currentActivity = context as? Activity
                        if (currentActivity != null && AdManager.interstitialAd != null) {
                            AdManager.interstitialAd?.fullScreenContentCallback = object : FullScreenContentCallback() {
                                override fun onAdDismissedFullScreenContent() {
                                    AdManager.interstitialAd = null
                                    AdManager.loadInterstitialAd(context)
                                    startExtractionProcess()
                                }
                                override fun onAdFailedToShowFullScreenContent(p0: AdError) {
                                    AdManager.interstitialAd = null
                                    startExtractionProcess()
                                }
                            }
                            AdManager.interstitialAd?.show(currentActivity)
                        } else {
                            AdManager.loadInterstitialAd(context)
                            startExtractionProcess()
                        }
                    }

                }) { Text("一瞬で開始する") }
            },
            dismissButton = {
                TextButton(onClick = { showFpsDialog = false; pendingVideoUri = null }) { Text("キャンセル") }
            }
        )
    }

    if (showRemoveAfterDialog) {
        AlertDialog(
            onDismissRequest = { showRemoveAfterDialog = false },
            title = { Text("確認", fontWeight = FontWeight.Bold) },
            text = { Text("このコマ以降のすべての追従枠を削除しますか？\n(※一度消すと元には戻せません)") },
            confirmButton = {
                Button(
                    onClick = {
                        for (i in uiState.currentFrameIndex until viewModel.frameFiles.size) viewModel.trackingMap.remove(i)
                        viewModel.userKeyframes.removeAll { it >= uiState.currentFrameIndex }
                        viewModel.interpolatedFrames.removeAll { it >= uiState.currentFrameIndex }
                        // [修正] saveProjectData を IO スレッドで実行
                        viewModel.viewModelScope.launch {
                            withContext(Dispatchers.IO) {
                                saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                            }
                        }
                        Toast.makeText(context, "これ以降を外しました", Toast.LENGTH_SHORT).show()
                        showRemoveAfterDialog = false
                    },
                    colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error)
                ) { Text("削除する", color = MaterialTheme.colorScheme.onError) }
            },
            dismissButton = {
                TextButton(onClick = { showRemoveAfterDialog = false }) { Text("キャンセル") }
            }
        )
    }

    if (showSaveSlotDialog) {
        val dummy = slotRefreshTrigger
        AlertDialog(
            onDismissRequest = { showSaveSlotDialog = false },
            title = { Text("保存先スロットを選択", fontWeight = FontWeight.Bold) },
            text = {
                Column {
                    Text("現在の編集状態を保存します。")
                    Spacer(modifier = Modifier.height(8.dp))
                    for (i in 1..3) {
                        val slotDir = File(context.filesDir, "slot_$i")
                        val slotExists = slotDir.exists()
                        val sizeMB = if (slotExists) getFolderSizeBytes(slotDir) / (1024 * 1024) else 0L
                        val label = if (slotExists) "スロット $i (上書き / 約${sizeMB}MB)" else "スロット $i に保存 (空)"
                        Button(
                            onClick = {
                                showSaveSlotDialog = false
                                viewModel.updateState { it.copy(appMode = AppMode.EXTRACTING, processingMessage = "スロット $i に保存中...\n(※数秒かかります)") }
                                viewModel.viewModelScope.launch {
                                    // [修正] saveProjectData を IO スレッドで実行
                                    withContext(Dispatchers.IO) {
                                        saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                    }
                                    val success = saveToSlot(context, i)
                                    slotRefreshTrigger++
                                    viewModel.updateState { it.copy(appMode = AppMode.EDITING) }
                                    if (success) Toast.makeText(context, "スロット $i に保存しました", Toast.LENGTH_SHORT).show()
                                    else Toast.makeText(context, "保存に失敗しました", Toast.LENGTH_SHORT).show()
                                }
                            },
                            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                            colors = ButtonDefaults.buttonColors(containerColor = if (slotExists) MaterialTheme.colorScheme.secondary else MaterialTheme.colorScheme.primary)
                        ) { Text(label) }
                    }
                }
            },
            confirmButton = {},
            dismissButton = { TextButton(onClick = { showSaveSlotDialog = false }) { Text("キャンセル") } }
        )
    }

    if (showLoadSlotDialog) {
        val dummy = slotRefreshTrigger
        AlertDialog(
            onDismissRequest = { showLoadSlotDialog = false },
            title = { Text("読み込むスロットを選択", fontWeight = FontWeight.Bold) },
            text = {
                Column {
                    Text("保存した状態を復元します。\n(※現在の編集は上書きされます)")
                    Spacer(modifier = Modifier.height(8.dp))
                    for (i in 1..3) {
                        val slotDir = File(context.filesDir, "slot_$i")
                        val slotExists = slotDir.exists()
                        val sizeMB = if (slotExists) getFolderSizeBytes(slotDir) / (1024 * 1024) else 0L
                        Button(
                            onClick = {
                                if (slotExists) {
                                    showLoadSlotDialog = false
                                    viewModel.updateState { it.copy(appMode = AppMode.EXTRACTING, processingMessage = "スロット $i から読込中...\n(※数秒かかります)") }
                                    viewModel.viewModelScope.launch {
                                        val success = loadFromSlot(context, i)
                                        if (success) {
                                            viewModel.evictAllBitmaps()
                                            previewScale = 1f
                                            offset = Offset.Zero
                                            val framesDir = File(context.filesDir, "extracted_frames")
                                            val files = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()
                                            viewModel.frameFiles.clear()
                                            viewModel.frameFiles.addAll(files)
                                            viewModel.allFramesObjects.clear()
                                            // [修正] loadProjectData を IO スレッドで実行
                                            val loadedFps = withContext(Dispatchers.IO) {
                                                loadProjectData(context, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                            }
                                            viewModel.updateState { it.copy(selectedFps = loadedFps, currentFrameIndex = 0, appMode = AppMode.EDITING) }
                                            Toast.makeText(context, "スロット $i を読み込みました", Toast.LENGTH_SHORT).show()
                                        } else {
                                            viewModel.updateState { it.copy(appMode = if(viewModel.frameFiles.isEmpty()) AppMode.SELECT_VIDEO else AppMode.EDITING) }
                                            Toast.makeText(context, "読込に失敗しました", Toast.LENGTH_SHORT).show()
                                        }
                                    }
                                } else {
                                    Toast.makeText(context, "このスロットは空です", Toast.LENGTH_SHORT).show()
                                }
                            },
                            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                            colors = ButtonDefaults.buttonColors(containerColor = if (slotExists) MaterialTheme.colorScheme.primary else Color.Gray)
                        ) { Text(if (slotExists) "スロット $i を開く (約${sizeMB}MB)" else "スロット $i (空)") }
                    }
                }
            },
            confirmButton = {},
            dismissButton = { TextButton(onClick = { showLoadSlotDialog = false }) { Text("キャンセル") } }
        )
    }

    if (showDeleteSlotDialog) {
        val dummy = slotRefreshTrigger
        AlertDialog(
            onDismissRequest = { showDeleteSlotDialog = false },
            title = { Text("保存データの削除", fontWeight = FontWeight.Bold) },
            text = {
                Column {
                    Text("スマホの容量を空けるため、不要なセーブデータを完全に削除します。")
                    Spacer(modifier = Modifier.height(8.dp))
                    for (i in 1..3) {
                        val slotDir = File(context.filesDir, "slot_$i")
                        val slotExists = slotDir.exists()
                        val sizeMB = if (slotExists) getFolderSizeBytes(slotDir) / (1024 * 1024) else 0L

                        Button(
                            onClick = {
                                if (slotExists) {
                                    slotDir.deleteRecursively()
                                    slotRefreshTrigger++
                                    Toast.makeText(context, "スロット $i を削除しました", Toast.LENGTH_SHORT).show()
                                }
                            },
                            modifier = Modifier.fillMaxWidth().padding(vertical = 4.dp),
                            colors = ButtonDefaults.buttonColors(containerColor = if (slotExists) MaterialTheme.colorScheme.error else Color.Gray),
                            enabled = slotExists
                        ) {
                            Text(if (slotExists) "スロット $i を削除 (約${sizeMB}MB)" else "スロット $i (空)", color = Color.White)
                        }
                    }
                }
            },
            confirmButton = {},
            dismissButton = { TextButton(onClick = { showDeleteSlotDialog = false }) { Text("閉じる") } }
        )
    }

    if (showHelpDialog) {
        Dialog(onDismissRequest = { showHelpDialog = false }, properties = DialogProperties(usePlatformDefaultWidth = false)) {
            Surface(modifier = Modifier.fillMaxSize().padding(16.dp), shape = RoundedCornerShape(16.dp), color = MaterialTheme.colorScheme.surface, tonalElevation = 8.dp) {
                Column(modifier = Modifier.padding(16.dp)) {
                    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween, verticalAlignment = Alignment.CenterVertically) {
                        Text("神動画を作る5つのコツ", style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.Bold)
                        IconButton(onClick = { showHelpDialog = false }) { Icon(Icons.Default.Close, contentDescription = "閉じる") }
                    }
                    Divider(modifier = Modifier.padding(vertical = 8.dp))

                    Column(modifier = Modifier.weight(1f).verticalScroll(rememberScrollState())) {
                        Text("1. 基本のキ！綺麗な追従のコツ", style = MaterialTheme.typography.titleMedium, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("❌ NG: 動画を読み込んで適当にタップする\n⭕ OK: 推しが一番「綺麗に全身（または上半身）が映っているコマ」でタップしてロックオン！そこから「タップで追従」をオンにしましょう。")
                        Spacer(modifier = Modifier.height(16.dp))

                        Text("2. 激しいダンスを撮る時", style = MaterialTheme.typography.titleMedium, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("❌ NG: カメラが推しの動きに合わせて揺れて酔う…\n⭕ OK: 詳細設定の「追従のスタイル」を「構図を固定 (ダンス等)」に変更！さらに手ブレ補正を「80%〜90%」に強めると、推しが画面中央でピタッと安定します。")
                        Spacer(modifier = Modifier.height(16.dp))

                        Text("3. メンバー同士が重なる時", style = MaterialTheme.typography.titleMedium, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("❌ NG: 見失うたびに最初からやり直し…\n⭕ OK: 隠れる直前で一時停止 ➡ 別のメンバーの後ろから「再び現れたコマ」まで進む ➡ そこで推しをタップして「隠れた間を繋ぐ」をオン！緑色の枠が自動で裏側を繋いでくれます")
                        Spacer(modifier = Modifier.height(16.dp))

                        Text("4. 全員同じ衣装でAIが迷う時", style = MaterialTheme.typography.titleMedium, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("❌ NG: 何度やっても隣のメンバーに枠が移る…\n⭕ OK: 設定の「枠の幅調整」を「50%」程度に狭くし、「追従の優先度」を「位置・予測を優先」に変更！これで隣の人との混同をガッチリ防げます")
                        Spacer(modifier = Modifier.height(16.dp))

                        Text("5. カメラの視点がパッと変わる時", style = MaterialTheme.typography.titleMedium, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.Bold)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("❌ NG: 枠が画面を横切ってビュンッと飛んでしまう\n⭕ OK: 切り替わった瞬間のコマで、紫色の「カット指定」ボタンをポチッ！カメラが「瞬間移動」して、自然な動画に仕上がります")
                        Spacer(modifier = Modifier.height(16.dp))
                    }
                    Button(onClick = { showHelpDialog = false }, modifier = Modifier.fillMaxWidth().padding(top = 8.dp)) { Text("閉じて編集に戻る") }
                }
            }
        }
    }

    Column(modifier = Modifier.padding(horizontal = 12.dp).fillMaxSize().verticalScroll(rememberScrollState()), horizontalAlignment = Alignment.CenterHorizontally) {
        Spacer(modifier = Modifier.height(4.dp))
        Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween, verticalAlignment = Alignment.CenterVertically) {
            Text("推しカメラ Pro", style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.ExtraBold, color = MaterialTheme.colorScheme.primary)
            Row(verticalAlignment = Alignment.CenterVertically) {
                IconButton(onClick = onThemeToggle) {
                    Text(if (isDarkTheme) "☀️" else "🌙", fontSize = 24.sp)
                }
                IconButton(onClick = { showHelpDialog = true }) {
                    Icon(Icons.Default.Info, contentDescription = "使い方", tint = MaterialTheme.colorScheme.primary)
                }
            }
        }
        Spacer(modifier = Modifier.height(4.dp))

        Button(
            onClick = { videoPickerLauncher.launch("video/*") },
            modifier = Modifier.fillMaxWidth().height(44.dp),
            enabled = (uiState.appMode == AppMode.SELECT_VIDEO || uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON),
            shape = RoundedCornerShape(12.dp),
            elevation = ButtonDefaults.buttonElevation(defaultElevation = 4.dp, pressedElevation = 0.dp)
        ) {
            Text(
                text = if (uiState.appMode == AppMode.SELECT_VIDEO) "動画を選択して開始する" else "別の動画に変更する",
                textAlign = TextAlign.Center,
                fontSize = 15.sp,
                fontWeight = FontWeight.Bold
            )
        }

        if (uiState.appMode == AppMode.SELECT_VIDEO || uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON) {
            Spacer(modifier = Modifier.height(8.dp))
            TopActionButtons(
                appMode = uiState.appMode,
                onSave = { showSaveSlotDialog = true },
                onLoad = { showLoadSlotDialog = true },
                onDelete = { showDeleteSlotDialog = true }
            )
        }

        if (uiState.isBackgroundExtracting) {
            Spacer(modifier = Modifier.height(4.dp))
            ElevatedCard(modifier = Modifier.fillMaxWidth(), colors = CardDefaults.elevatedCardColors(containerColor = MaterialTheme.colorScheme.secondaryContainer)) {
                Column(modifier = Modifier.padding(8.dp).fillMaxWidth(), horizontalAlignment = Alignment.CenterHorizontally) {
                    Text("裏側で動画の残りを準備中...", fontSize = 12.sp, fontWeight = FontWeight.Bold, color = MaterialTheme.colorScheme.onSecondaryContainer)
                    Spacer(modifier = Modifier.height(6.dp))
                    LinearProgressIndicator(progress = uiState.bgExtractProgress / 100f, modifier = Modifier.fillMaxWidth().height(6.dp).clip(RoundedCornerShape(3.dp)), color = MaterialTheme.colorScheme.primary)
                    Spacer(modifier = Modifier.height(2.dp))
                    Text("${uiState.bgExtractProgress}%", fontSize = 10.sp, color = MaterialTheme.colorScheme.onSecondaryContainer)
                }
            }
        }

        Spacer(modifier = Modifier.height(6.dp))

        ElevatedCard(
            modifier = Modifier.fillMaxWidth().height(240.dp),
            shape = RoundedCornerShape(16.dp),
            elevation = CardDefaults.elevatedCardElevation(defaultElevation = 6.dp)
        ) {
            Box(
                modifier = Modifier.fillMaxSize().background(Color.Black).clipToBounds()
                    .pointerInput(Unit) { detectTransformGestures { _, pan, zoom, _ -> previewScale = (previewScale * zoom).coerceIn(1f, 10f); offset = if (previewScale == 1f) Offset.Zero else offset + pan } }
                    .pointerInput(uiState, previewBitmap) {
                        detectTapGestures { tapOffset ->
                            if (uiState.isPlaying || previewBitmap == null || uiState.appMode == AppMode.TRACKING || uiState.appMode == AppMode.EXPORTING || uiState.appMode == AppMode.EXTRACTING) return@detectTapGestures

                            val canvasWidth = size.width.toFloat()
                            val canvasHeight = size.height.toFloat()
                            val unscaledTapX = ((tapOffset.x - canvasWidth / 2f) - offset.x) / previewScale + canvasWidth / 2f
                            val unscaledTapY = ((tapOffset.y - canvasHeight / 2f) - offset.y) / previewScale + canvasHeight / 2f
                            val imageScale = min(canvasWidth / uiState.videoWidth, canvasHeight / uiState.videoHeight)
                            val offsetXBase = (canvasWidth - uiState.videoWidth * imageScale) / 2f
                            val offsetYBase = (canvasHeight - uiState.videoHeight * imageScale) / 2f
                            val tapX = (unscaledTapX - offsetXBase) / imageScale
                            val tapY = (unscaledTapY - offsetYBase) / imageScale

                            val candidates = if (uiState.isManualBoxAddEnabled) emptyList() else (viewModel.allFramesObjects[uiState.currentFrameIndex] ?: emptyList())
                            var tappedBox = candidates.find { it.contains(tapX.toInt(), tapY.toInt()) }

                            var isManualBox = false
                            if (tappedBox == null) {
                                if (uiState.isManualBoxAddEnabled) {
                                    val defaultSizeX = uiState.videoWidth * (uiState.manualBoxSizePercent / 100f) * (uiState.boxWidthPercent / 100f)
                                    val defaultSizeY = uiState.videoWidth * (uiState.manualBoxSizePercent / 100f) * 3.0f
                                    tappedBox = Rect(
                                        (tapX - defaultSizeX / 2).toInt().coerceAtLeast(0),
                                        (tapY - defaultSizeY / 2).toInt().coerceAtLeast(0),
                                        (tapX + defaultSizeX / 2).toInt().coerceAtMost(uiState.videoWidth.toInt()),
                                        (tapY + defaultSizeY / 2).toInt().coerceAtMost(uiState.videoHeight.toInt())
                                    )
                                    isManualBox = true
                                } else {
                                    return@detectTapGestures
                                }
                            }

                            if (tappedBox != null) {
                                if (uiState.isInterpolationEnabled) {
                                    val prevValidFrame = viewModel.trackingMap.keys.filter { it < uiState.currentFrameIndex }.maxOrNull()
                                    if (prevValidFrame != null && viewModel.trackingMap[prevValidFrame] != null) {
                                        val startBox = viewModel.trackingMap[prevValidFrame]!!
                                        val endBox = tappedBox
                                        val steps = uiState.currentFrameIndex - prevValidFrame
                                        for (i in 1 until steps) {
                                            val ratio = i.toFloat() / steps.toFloat()
                                            val interpLeft = startBox.left + (endBox.left - startBox.left) * ratio
                                            val interpTop = startBox.top + (endBox.top - startBox.top) * ratio
                                            val interpRight = startBox.right + (endBox.right - startBox.right) * ratio
                                            val interpBottom = startBox.bottom + (endBox.bottom - startBox.bottom) * ratio
                                            viewModel.trackingMap[prevValidFrame + i] = Rect(interpLeft.toInt(), interpTop.toInt(), interpRight.toInt(), interpBottom.toInt())
                                            if (!viewModel.interpolatedFrames.contains(prevValidFrame + i)) {
                                                viewModel.interpolatedFrames.add(prevValidFrame + i)
                                            }
                                        }
                                        if (!viewModel.interpolatedFrames.contains(uiState.currentFrameIndex)) {
                                            viewModel.interpolatedFrames.add(uiState.currentFrameIndex)
                                        }
                                        Toast.makeText(context, "${steps - 1}コマの間を緑色の枠で繋ぎました！", Toast.LENGTH_SHORT).show()
                                    } else {
                                        Toast.makeText(context, "繋ぐための「前の枠」がありません", Toast.LENGTH_SHORT).show()
                                    }
                                } else {
                                    viewModel.interpolatedFrames.remove(uiState.currentFrameIndex)
                                    if (isManualBox) {
                                        Toast.makeText(context, "手動で枠を作成しました", Toast.LENGTH_SHORT).show()
                                    }
                                }

                                viewModel.trackingMap[uiState.currentFrameIndex] = tappedBox
                                if (!viewModel.userKeyframes.contains(uiState.currentFrameIndex)) viewModel.userKeyframes.add(uiState.currentFrameIndex)

                                if (uiState.autoTrackOnTap) {
                                    viewModel.updateState { it.copy(appMode = AppMode.TRACKING, isTrackingCancelled = false) }
                                    val safeStartBox = Rect(tappedBox)
                                    val safeFramesObjects = viewModel.allFramesObjects.toMap()

                                    viewModel.viewModelScope.launch {
                                        try {
                                            val framesDir = File(context.filesDir, "extracted_frames")
                                            val lostFrame = runAutoTracking(
                                                startStep = uiState.currentFrameIndex,
                                                startBox = safeStartBox,
                                                framesDir = framesDir,
                                                estimatedTotal = max(uiState.estimatedTotalFrames, viewModel.frameFiles.size),
                                                isBgExtracting = { uiState.isBackgroundExtracting },
                                                allFramesObjectsCopy = safeFramesObjects,
                                                trackingPriority = uiState.selectedTrackingPriority,
                                                boxWidthPercent = uiState.boxWidthPercent,
                                                pauseOnLostTracker = uiState.pauseOnLostTracker,
                                                onProgress = { msg -> viewModel.updateState { it.copy(processingMessage = msg) } },
                                                onFrameUpdate = { step, box, newFilteredObjs, uiBmp ->
                                                    viewModel.updateState { it.copy(currentFrameIndex = step) }
                                                    if (box != null) {
                                                        viewModel.trackingMap[step] = box
                                                    } else {
                                                        viewModel.trackingMap.remove(step)
                                                    }
                                                    if (newFilteredObjs != null) viewModel.allFramesObjects[step] = newFilteredObjs
                                                    if (uiBmp != null) {
                                                        viewModel.putBitmap(step, uiBmp)
                                                        previewBitmap = uiBmp
                                                    }
                                                },
                                                isCancelled = { viewModel.uiState.value.isTrackingCancelled }
                                            )

                                            viewModel.updateState { it.copy(isInterpolationEnabled = false, autoTrackOnTap = false, appMode = AppMode.EDITING) }
                                            // [修正] saveProjectData を IO スレッドで実行
                                            withContext(Dispatchers.IO) {
                                                saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                            }

                                            if (viewModel.uiState.value.isTrackingCancelled) {
                                                Toast.makeText(context, "追従を中断しました", Toast.LENGTH_SHORT).show()
                                            } else if (lostFrame != null) {
                                                viewModel.updateState { it.copy(currentFrameIndex = lostFrame) }
                                                Toast.makeText(context, "見失ったため、${lostFrame}コマ目で停止しました。再度枠を指定してください。", Toast.LENGTH_LONG).show()
                                            } else {
                                                Toast.makeText(context, "追従完了！", Toast.LENGTH_SHORT).show()
                                            }
                                        } catch (e: Exception) {
                                            e.printStackTrace()
                                            viewModel.updateState { it.copy(appMode = AppMode.EDITING) }
                                            Toast.makeText(context, "エラー停止: ${e.message}", Toast.LENGTH_LONG).show()
                                        }
                                    }
                                } else {
                                    if (!uiState.isInterpolationEnabled && !isManualBox) {
                                        Toast.makeText(context, "1コマ枠を設定しました", Toast.LENGTH_SHORT).show()
                                    }
                                    viewModel.updateState { it.copy(isInterpolationEnabled = false) }
                                    // [修正] saveProjectData を IO スレッドで実行
                                    viewModel.viewModelScope.launch {
                                        withContext(Dispatchers.IO) {
                                            saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                        }
                                    }
                                }
                            }
                        }
                    },
                contentAlignment = Alignment.Center
            ) {
                if (previewBitmap != null) {
                    Box(modifier = Modifier.fillMaxSize().graphicsLayer(scaleX = previewScale, scaleY = previewScale, translationX = offset.x, translationY = offset.y)) {
                        Image(bitmap = previewBitmap!!.asImageBitmap(), contentDescription = null, modifier = Modifier.fillMaxSize(), contentScale = ContentScale.Fit)
                        Canvas(modifier = Modifier.fillMaxSize().clipToBounds()) {
                            val imageScale = min(size.width / uiState.videoWidth, size.height / uiState.videoHeight)
                            val offsetXBase = (size.width - uiState.videoWidth * imageScale) / 2f
                            val offsetYBase = (size.height - uiState.videoHeight * imageScale) / 2f

                            if (!uiState.isPlaying && !uiState.isManualBoxAddEnabled && uiState.appMode != AppMode.EXTRACTING) {
                                viewModel.allFramesObjects[uiState.currentFrameIndex]?.forEach { box ->
                                    if (viewModel.trackingMap[uiState.currentFrameIndex] != box) {
                                        drawRect(
                                            color = Color.Yellow,
                                            topLeft = Offset(box.left * imageScale + offsetXBase, box.top * imageScale + offsetYBase),
                                            size = Size(box.width() * imageScale, box.height() * imageScale),
                                            style = Stroke(width = 2f / previewScale)
                                        )
                                    }
                                }
                            }

                            if (uiState.appMode != AppMode.EXTRACTING) {
                                viewModel.trackingMap[uiState.currentFrameIndex]?.let { box ->
                                    val boxColor = when {
                                        viewModel.cutFrames.contains(uiState.currentFrameIndex) -> Color(0xFFE040FB)
                                        viewModel.interpolatedFrames.contains(uiState.currentFrameIndex) -> Color(0xFF00E676)
                                        else -> Color.Red
                                    }
                                    drawRect(
                                        color = boxColor,
                                        topLeft = Offset(box.left * imageScale + offsetXBase, box.top * imageScale + offsetYBase),
                                        size = Size(box.width() * imageScale, box.height() * imageScale),
                                        style = Stroke(width = 4f / previewScale)
                                    )
                                }
                            }

                            if (showCameraPreviewBox && viewModel.frameFiles.isNotEmpty() && viewModel.trackingMap.isNotEmpty() && uiState.appMode != AppMode.EXTRACTING && uiState.appMode != AppMode.TRACKING) {
                                val targetRatio = when (uiState.selectedRatio) {
                                    "スマホ画面に合わせる (フル)" -> {
                                        val metrics = context.resources.displayMetrics
                                        val w = min(metrics.widthPixels, metrics.heightPixels).toFloat()
                                        val h = max(metrics.widthPixels, metrics.heightPixels).toFloat()
                                        w / h
                                    }
                                    "9:16 (TikTok等)" -> 9f / 16f; "16:9 (YouTube等)" -> 16f / 9f; "4:3 (レトロ)" -> 4f / 3f; "1:1 (正方形)" -> 1f; else -> 9f / 16f
                                }

                                var cropW: Float; var cropH: Float
                                if (uiState.videoWidth / uiState.videoHeight > targetRatio) {
                                    cropH = uiState.videoHeight * (uiState.cropZoomPercent / 100f)
                                    cropW = cropH * targetRatio
                                } else {
                                    cropW = uiState.videoWidth * (uiState.cropZoomPercent / 100f)
                                    cropH = cropW / targetRatio
                                }

                                val smoothingBase = (100f - uiState.shakeReduction) / 100f * 0.1f + 0.01f
                                val isDanceMode = (uiState.selectedTrackingMode == "構図を固定 (ダンス等)")

                                var camX = viewModel.trackingMap[0]?.centerX()?.toFloat() ?: (uiState.videoWidth / 2f)
                                var camY = if (uiState.selectedTrackAxis == "横移動のみ (縦固定)") (uiState.videoHeight / 2f) else (viewModel.trackingMap[0]?.centerY()?.toFloat() ?: (uiState.videoHeight / 2f))

                                for (i in 0..uiState.currentFrameIndex) {
                                    val box = viewModel.trackingMap[i]
                                    if (box != null) {
                                        val targetX = box.centerX().toFloat()
                                        val targetY = box.centerY().toFloat()

                                        if (viewModel.cutFrames.contains(i)) {
                                            camX = targetX; if (uiState.selectedTrackAxis == "上下左右 (通常)") camY = targetY
                                        } else {
                                            val distX = abs(targetX - camX)
                                            val distY = abs(targetY - camY)

                                            val normDistX = distX / cropW
                                            val normDistY = distY / cropH
                                            val boostX = if (normDistX > 0.1f) (normDistX - 0.1f) * 2.5f else 0f
                                            val boostY = if (normDistY > 0.1f) (normDistY - 0.1f) * 2.5f else 0f

                                            val speedFactorX = if (isDanceMode && normDistX < 0.08f) 0.1f else 1.0f
                                            val speedFactorY = if (isDanceMode && normDistY < 0.08f) 0.1f else 1.0f

                                            val finalSpeedX = ((smoothingBase + boostX) * speedFactorX).coerceIn(0.01f, 1f)
                                            val finalSpeedY = ((smoothingBase + boostY) * speedFactorY).coerceIn(0.01f, 1f)

                                            camX += (targetX - camX) * finalSpeedX
                                            if (uiState.selectedTrackAxis == "上下左右 (通常)") {
                                                camY += (targetY - camY) * finalSpeedY
                                            } else {
                                                camY = uiState.videoHeight / 2f
                                            }
                                        }
                                    }
                                }

                                val safeX = (camX - cropW / 2).coerceIn(0f, max(0f, uiState.videoWidth - cropW))
                                val safeY = (camY - cropH / 2).coerceIn(0f, max(0f, uiState.videoHeight - cropH))

                                val camLeft = safeX * imageScale + offsetXBase
                                val camTop = safeY * imageScale + offsetYBase
                                val camRight = (safeX + cropW) * imageScale + offsetXBase
                                val camBottom = (safeY + cropH) * imageScale + offsetYBase

                                val bgPath = Path().apply {
                                    addRect(androidx.compose.ui.geometry.Rect(0f, 0f, size.width, size.height))
                                    addRect(androidx.compose.ui.geometry.Rect(camLeft, camTop, camRight, camBottom))
                                    fillType = PathFillType.EvenOdd
                                }
                                drawPath(bgPath, color = Color.Black.copy(alpha = 0.6f))
                                drawRect(color = Color(0xFF00E5FF), topLeft = Offset(camLeft, camTop), size = Size(camRight - camLeft, camBottom - camTop), style = Stroke(width = 3f / previewScale))
                            }
                        }
                    }

                    if (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON) {
                        IconButton(
                            onClick = { showCameraPreviewBox = !showCameraPreviewBox },
                            modifier = Modifier.align(Alignment.TopEnd).padding(8.dp).background(Color.Black.copy(alpha = 0.5f), shape = RoundedCornerShape(8.dp))
                        ) { Text(if (showCameraPreviewBox) "👁️" else "🙈", fontSize = 20.sp) }
                    }
                } else {
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        Icon(Icons.Default.PlayArrow, contentDescription = null, modifier = Modifier.size(48.dp), tint = Color.DarkGray)
                        Spacer(modifier = Modifier.height(12.dp))
                        Text("動画が選択されていません", color = Color.LightGray, fontWeight = FontWeight.Bold, fontSize = 15.sp)
                        Spacer(modifier = Modifier.height(4.dp))
                        Text("上のボタンから動画を読み込むと\nここにプレビューが表示されます", color = Color.Gray, fontSize = 12.sp, textAlign = TextAlign.Center)
                    }
                }
            }
        }

        if (viewModel.frameFiles.isNotEmpty()) {
            Spacer(modifier = Modifier.height(6.dp))
            TrackingSwitchPanel(uiState = uiState, viewModel = viewModel)
            PlaybackControlPanel(uiState = uiState, viewModel = viewModel, onRemoveAfterDialog = { showRemoveAfterDialog = true }, context = context)
        }

        Spacer(modifier = Modifier.height(8.dp))

        if (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON) {
            SettingsCard(
                uiState = uiState,
                viewModel = viewModel,
                onExport = exportLabel@{
                    if (uiState.isBackgroundExtracting) {
                        Toast.makeText(context, "動画の展開が完了するまでお待ちください", Toast.LENGTH_SHORT).show()
                        return@exportLabel
                    }

                    val usableSpace = context.cacheDir.usableSpace
                    val bytesPerFrame = when (uiState.selectedResolution) {
                        "4K (Ultra HD)" -> 1200_000L
                        "2K (QHD)" -> 600_000L
                        "1080p (Full HD)" -> 300_000L
                        else -> 150_000L
                    }
                    val exportFps = if (uiState.isSmoothExport && uiState.selectedFps < 30) 30 else uiState.selectedFps
                    val estimatedExportFrames = viewModel.frameFiles.size * (exportFps.toFloat() / uiState.selectedFps.toFloat())
                    val requiredSpace = (estimatedExportFrames * bytesPerFrame).toLong() + 100_000_000L

                    if (usableSpace < requiredSpace) {
                        val reqMB = requiredSpace / (1024 * 1024)
                        val freeMB = usableSpace / (1024 * 1024)
                        Toast.makeText(context, "空き容量が不足しています！\n必要: 約${reqMB}MB / 空き: 約${freeMB}MB", Toast.LENGTH_LONG).show()
                        return@exportLabel
                    }

                    val startExportProcess = {
                        if (!uiState.isPremiumVersion) {
                            if (uiState.selectedResolution != "720p (SNS向け・爆速)") {
                                Toast.makeText(context, "無料版の高画質出力は最初の5秒間(透かし有)のお試しになります", Toast.LENGTH_LONG).show()
                            } else {
                                Toast.makeText(context, "無料版のためロゴ透かしが入ります", Toast.LENGTH_LONG).show()
                            }
                        }

                        viewModel.updateState { it.copy(appMode = AppMode.EXPORTING, isTrackingCancelled = false) }
                        viewModel.viewModelScope.launch {
                            val originalVideoFile = File(context.filesDir, "working_video.mp4")
                            val output = processVideoProfessional(
                                context, originalVideoFile, viewModel.frameFiles, viewModel.trackingMap,
                                uiState.selectedRatio, uiState.cropZoomPercent, uiState.selectedFps, uiState.isSmoothExport,
                                uiState.selectedTrackAxis, uiState.selectedTrackingMode, uiState.selectedResolution,
                                uiState.shakeReduction, viewModel.cutFrames, uiState.isPremiumVersion,
                                onProgress = { msg -> viewModel.updateState { it.copy(processingMessage = msg) } },
                                contextForToast = context, viewModel = viewModel,
                                isCancelled = { viewModel.uiState.value.isTrackingCancelled }
                            )

                            if (output != null) {
                                saveVideoToGallery(context, output)
                                Toast.makeText(context, "保存完了！カメラロールを確認してください", Toast.LENGTH_LONG).show()
                            } else if (viewModel.uiState.value.isTrackingCancelled) {
                                Toast.makeText(context, "書き出しを中断しました", Toast.LENGTH_LONG).show()
                            } else {
                                Toast.makeText(context, "出力エラー、または安全装置が作動しました。", Toast.LENGTH_LONG).show()
                            }
                            viewModel.updateState { it.copy(appMode = AppMode.EDITING) }
                        }
                    }

                    if (uiState.isPremiumVersion) {
                        startExportProcess()
                    } else {
                        val currentActivity = context as? Activity
                        if (currentActivity != null && AdManager.rewardedAd != null) {
                            var rewardEarned = false
                            AdManager.rewardedAd?.fullScreenContentCallback = object : FullScreenContentCallback() {
                                override fun onAdDismissedFullScreenContent() {
                                    AdManager.rewardedAd = null
                                    AdManager.loadRewardedAd(context)
                                    if (!rewardEarned) {
                                        Toast.makeText(context, "広告を最後まで視聴すると書き出しが始まります", Toast.LENGTH_SHORT).show()
                                    }
                                }
                                override fun onAdFailedToShowFullScreenContent(p0: AdError) {
                                    AdManager.rewardedAd = null
                                    startExportProcess()
                                }
                            }
                            AdManager.rewardedAd?.show(currentActivity) { _ ->
                                rewardEarned = true
                                startExportProcess()
                            }
                        } else {
                            AdManager.loadRewardedAd(context)
                            startExportProcess()
                        }
                    }
                },
                context = context
            )
        }

        if (uiState.appMode == AppMode.TRACKING || uiState.appMode == AppMode.EXPORTING || uiState.appMode == AppMode.EXTRACTING) {
            Spacer(modifier = Modifier.height(8.dp))
            ElevatedCard(
                modifier = Modifier.fillMaxWidth(),
                colors = CardDefaults.elevatedCardColors(containerColor = MaterialTheme.colorScheme.errorContainer),
                elevation = CardDefaults.elevatedCardElevation(defaultElevation = 8.dp)
            ) {
                Column(modifier = Modifier.padding(12.dp), horizontalAlignment = Alignment.CenterHorizontally) {
                    CircularProgressIndicator(color = MaterialTheme.colorScheme.error, modifier = Modifier.size(36.dp))
                    Spacer(modifier = Modifier.height(8.dp))
                    Text(text = uiState.processingMessage, color = MaterialTheme.colorScheme.onErrorContainer, fontWeight = FontWeight.Bold, textAlign = TextAlign.Center, lineHeight = 20.sp)

                    if (uiState.appMode == AppMode.TRACKING || uiState.appMode == AppMode.EXPORTING) {
                        Spacer(modifier = Modifier.height(8.dp))
                        Button(
                            onClick = {
                                viewModel.updateState { it.copy(isTrackingCancelled = true) }
                                if (uiState.appMode == AppMode.EXPORTING) {
                                    viewModel.cancelFfmpegProcess()
                                }
                                Toast.makeText(context, "停止リクエストを送信しました...", Toast.LENGTH_SHORT).show()
                            },
                            colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error),
                            modifier = Modifier.fillMaxWidth().height(40.dp),
                            shape = RoundedCornerShape(12.dp)
                        ) {
                            Icon(Icons.Default.Close, contentDescription = "停止", tint = MaterialTheme.colorScheme.onError)
                            Spacer(modifier = Modifier.width(8.dp))
                            Text(if(uiState.appMode == AppMode.TRACKING) "追従を安全に停止" else "書き出しを安全に停止", color = MaterialTheme.colorScheme.onError, fontSize = 15.sp, fontWeight = FontWeight.Bold)
                        }
                    }
                }
            }
        }
        Spacer(modifier = Modifier.height(12.dp))
    }
}

// --- 分割されたUIコンポーネント ---

@Composable
fun TopActionButtons(appMode: AppMode, onSave: () -> Unit, onLoad: () -> Unit, onDelete: () -> Unit) {
    Spacer(modifier = Modifier.height(4.dp))
    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        FilledTonalButton(
            onClick = onSave,
            modifier = Modifier.weight(1f).height(36.dp),
            enabled = (appMode == AppMode.EDITING || appMode == AppMode.LOCK_ON),
            contentPadding = PaddingValues(0.dp),
            shape = RoundedCornerShape(8.dp)
        ) { Text("保存", fontSize = 13.sp, fontWeight = FontWeight.Bold) }
        FilledTonalButton(
            onClick = onLoad,
            modifier = Modifier.weight(1f).height(36.dp),
            contentPadding = PaddingValues(0.dp),
            shape = RoundedCornerShape(8.dp)
        ) { Text("読込", fontSize = 13.sp, fontWeight = FontWeight.Bold) }
        OutlinedButton(
            onClick = onDelete,
            modifier = Modifier.weight(1f).height(36.dp),
            contentPadding = PaddingValues(0.dp),
            shape = RoundedCornerShape(8.dp)
        ) { Text("消去", fontSize = 13.sp, color = MaterialTheme.colorScheme.error, fontWeight = FontWeight.Bold) }
    }
}

@Composable
fun TrackingSwitchPanel(uiState: OshiCamUiState, viewModel: OshiCamViewModel) {
    ElevatedCard(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.elevatedCardElevation(defaultElevation = 2.dp)) {
        Row(modifier = Modifier.fillMaxWidth().padding(4.dp), horizontalArrangement = Arrangement.spacedBy(4.dp)) {
            Column(modifier = Modifier.weight(1f)) {
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(autoTrackOnTap = !it.autoTrackOnTap) } }) {
                    Switch(checked = uiState.autoTrackOnTap, onCheckedChange = { viewModel.updateState { state -> state.copy(autoTrackOnTap = it) } }, modifier = Modifier.scale(0.7f))
                    Text("タップで追従", style = MaterialTheme.typography.bodySmall, color = if(uiState.autoTrackOnTap) MaterialTheme.colorScheme.primary else Color.Gray, maxLines = 1, fontWeight = FontWeight.Bold)
                }
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(isManualBoxAddEnabled = !it.isManualBoxAddEnabled) } }) {
                    Switch(checked = uiState.isManualBoxAddEnabled, onCheckedChange = { viewModel.updateState { state -> state.copy(isManualBoxAddEnabled = it) } }, modifier = Modifier.scale(0.7f))
                    Text("枠を手で追加", style = MaterialTheme.typography.bodySmall, color = if(uiState.isManualBoxAddEnabled) MaterialTheme.colorScheme.primary else Color.Gray, maxLines = 1, fontWeight = FontWeight.Bold)
                }
            }
            Column(modifier = Modifier.weight(1f)) {
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(isInterpolationEnabled = !it.isInterpolationEnabled) } }) {
                    Switch(checked = uiState.isInterpolationEnabled, onCheckedChange = { viewModel.updateState { state -> state.copy(isInterpolationEnabled = it) } }, modifier = Modifier.scale(0.7f), colors = SwitchDefaults.colors(checkedThumbColor = Color(0xFF00E676), checkedTrackColor = Color(0xFF00E676).copy(alpha = 0.5f)))
                    Text("隠れた間を繋ぐ", style = MaterialTheme.typography.bodySmall.copy(fontWeight = if (uiState.isInterpolationEnabled) FontWeight.Bold else FontWeight.Normal), color = if (uiState.isInterpolationEnabled) Color(0xFF00C853) else Color.Gray, maxLines = 1)
                }
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(pauseOnLostTracker = !it.pauseOnLostTracker) } }) {
                    Switch(checked = uiState.pauseOnLostTracker, onCheckedChange = { viewModel.updateState { state -> state.copy(pauseOnLostTracker = it) } }, modifier = Modifier.scale(0.7f))
                    Text("見失ったら確認", style = MaterialTheme.typography.bodySmall, color = if(uiState.pauseOnLostTracker) MaterialTheme.colorScheme.primary else Color.Gray, maxLines = 1, fontWeight = FontWeight.Bold)
                }
            }
        }
    }
}

@Composable
fun PlaybackControlPanel(uiState: OshiCamUiState, viewModel: OshiCamViewModel, onRemoveAfterDialog: () -> Unit, context: Context) {
    val prevInteractionSource = remember { MutableInteractionSource() }
    val isPrevPressed by prevInteractionSource.collectIsPressedAsState()
    var didPrevLongPress by remember { mutableStateOf(false) }

    val nextInteractionSource = remember { MutableInteractionSource() }
    val isNextPressed by nextInteractionSource.collectIsPressedAsState()
    var didNextLongPress by remember { mutableStateOf(false) }

    val upInteractionSource = remember { MutableInteractionSource() }
    val isUpPressed by upInteractionSource.collectIsPressedAsState()
    var didUpLongPress by remember { mutableStateOf(false) }

    val downInteractionSource = remember { MutableInteractionSource() }
    val isDownPressed by downInteractionSource.collectIsPressedAsState()
    var didDownLongPress by remember { mutableStateOf(false) }

    val moveStepLong = 5
    val moveStepTap = 2

    LaunchedEffect(isPrevPressed) {
        if (isPrevPressed) {
            didPrevLongPress = false
            delay(300)
            if (isActive && isPrevPressed) {
                didPrevLongPress = true
                while (isActive && isPrevPressed) {
                    if (uiState.isBoxEditMode && !uiState.isPlaying) {
                        viewModel.moveCurrentBox(-moveStepLong, 0, uiState.videoWidth, uiState.videoHeight)
                    } else if (!uiState.isBoxEditMode) {
                        if (uiState.currentFrameIndex > 0) viewModel.updateState { it.copy(currentFrameIndex = max(0, it.currentFrameIndex - 1)) }
                    }
                    delay(50)
                }
            }
        } else {
            if (didPrevLongPress && uiState.isBoxEditMode) {
                // [修正] saveProjectData を IO スレッドで実行
                withContext(Dispatchers.IO) {
                    saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                }
            }
            didPrevLongPress = false
        }
    }

    LaunchedEffect(isNextPressed) {
        if (isNextPressed) {
            didNextLongPress = false
            delay(300)
            if (isActive && isNextPressed) {
                didNextLongPress = true
                while (isActive && isNextPressed) {
                    if (uiState.isBoxEditMode && !uiState.isPlaying) {
                        viewModel.moveCurrentBox(moveStepLong, 0, uiState.videoWidth, uiState.videoHeight)
                    } else if (!uiState.isBoxEditMode) {
                        if (uiState.currentFrameIndex < viewModel.frameFiles.size - 1) viewModel.updateState { it.copy(currentFrameIndex = min(viewModel.frameFiles.size - 1, it.currentFrameIndex + 1)) }
                    }
                    delay(50)
                }
            }
        } else {
            if (didNextLongPress && uiState.isBoxEditMode) {
                // [修正] saveProjectData を IO スレッドで実行
                withContext(Dispatchers.IO) {
                    saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                }
            }
            didNextLongPress = false
        }
    }

    LaunchedEffect(isUpPressed) {
        if (isUpPressed) {
            didUpLongPress = false; delay(300)
            if (isActive && isUpPressed) {
                didUpLongPress = true
                while (isActive && isUpPressed) {
                    if (!uiState.isPlaying) viewModel.moveCurrentBox(0, -moveStepLong, uiState.videoWidth, uiState.videoHeight)
                    delay(50)
                }
            }
        } else {
            if (didUpLongPress) {
                // [修正] saveProjectData を IO スレッドで実行
                withContext(Dispatchers.IO) {
                    saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                }
            }
            didUpLongPress = false
        }
    }

    LaunchedEffect(isDownPressed) {
        if (isDownPressed) {
            didDownLongPress = false; delay(300)
            if (isActive && isDownPressed) {
                didDownLongPress = true
                while (isActive && isDownPressed) {
                    if (!uiState.isPlaying) viewModel.moveCurrentBox(0, moveStepLong, uiState.videoWidth, uiState.videoHeight)
                    delay(50)
                }
            }
        } else {
            if (didDownLongPress) {
                // [修正] saveProjectData を IO スレッドで実行
                withContext(Dispatchers.IO) {
                    saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                }
            }
            didDownLongPress = false
        }
    }

    Spacer(modifier = Modifier.height(4.dp))

    ElevatedCard(modifier = Modifier.fillMaxWidth(), elevation = CardDefaults.elevatedCardElevation(defaultElevation = 2.dp)) {
        Column(modifier = Modifier.padding(8.dp)) {

            Row(modifier = Modifier.fillMaxWidth().padding(bottom = 6.dp), horizontalArrangement = Arrangement.SpaceBetween, verticalAlignment = Alignment.CenterVertically) {
                Text(
                    text = if (uiState.isBoxEditMode) "枠の微調整モード" else "コマ送り・再生モード",
                    style = MaterialTheme.typography.labelMedium,
                    color = if(uiState.isBoxEditMode) MaterialTheme.colorScheme.tertiary else Color.Gray,
                    fontWeight = FontWeight.Bold
                )
                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.clickable { viewModel.updateState { it.copy(isBoxEditMode = !it.isBoxEditMode) } }) {
                    Text("枠を移動する", fontSize = 12.sp, fontWeight = FontWeight.Bold, color = if(uiState.isBoxEditMode) MaterialTheme.colorScheme.tertiary else Color.Gray)
                    Spacer(modifier = Modifier.width(4.dp))
                    Switch(
                        checked = uiState.isBoxEditMode,
                        onCheckedChange = { viewModel.updateState { state -> state.copy(isBoxEditMode = it) } },
                        modifier = Modifier.scale(0.6f),
                        colors = SwitchDefaults.colors(checkedThumbColor = MaterialTheme.colorScheme.tertiary, checkedTrackColor = MaterialTheme.colorScheme.tertiaryContainer)
                    )
                }
            }

            if (uiState.isBoxEditMode) {
                Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                    Button(
                        onClick = {
                            if (!didPrevLongPress && !uiState.isPlaying) {
                                viewModel.moveCurrentBox(-moveStepTap, 0, uiState.videoWidth, uiState.videoHeight)
                                // [修正] saveProjectData を IO スレッドで実行
                                viewModel.viewModelScope.launch {
                                    withContext(Dispatchers.IO) {
                                        saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                    }
                                }
                            }
                        },
                        interactionSource = prevInteractionSource,
                        enabled = !uiState.isPlaying && viewModel.trackingMap[uiState.currentFrameIndex] != null,
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer, contentColor = MaterialTheme.colorScheme.onTertiaryContainer),
                        modifier = Modifier.weight(1f).height(48.dp),
                        shape = RoundedCornerShape(12.dp)
                    ) { Text("◀ 左", fontSize = 14.sp, fontWeight = FontWeight.ExtraBold) }

                    Column(modifier = Modifier.weight(1.2f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        Button(
                            onClick = {
                                if (!didUpLongPress && !uiState.isPlaying) {
                                    viewModel.moveCurrentBox(0, -moveStepTap, uiState.videoWidth, uiState.videoHeight)
                                    viewModel.viewModelScope.launch {
                                        withContext(Dispatchers.IO) {
                                            saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                        }
                                    }
                                }
                            },
                            interactionSource = upInteractionSource,
                            enabled = !uiState.isPlaying && viewModel.trackingMap[uiState.currentFrameIndex] != null,
                            colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer, contentColor = MaterialTheme.colorScheme.onTertiaryContainer),
                            modifier = Modifier.fillMaxWidth().height(22.dp),
                            contentPadding = PaddingValues(0.dp),
                            shape = RoundedCornerShape(8.dp)
                        ) { Text("▲ 上", fontSize = 11.sp, fontWeight = FontWeight.Bold) }

                        Button(
                            onClick = {
                                if (!didDownLongPress && !uiState.isPlaying) {
                                    viewModel.moveCurrentBox(0, moveStepTap, uiState.videoWidth, uiState.videoHeight)
                                    viewModel.viewModelScope.launch {
                                        withContext(Dispatchers.IO) {
                                            saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                        }
                                    }
                                }
                            },
                            interactionSource = downInteractionSource,
                            enabled = !uiState.isPlaying && viewModel.trackingMap[uiState.currentFrameIndex] != null,
                            colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer, contentColor = MaterialTheme.colorScheme.onTertiaryContainer),
                            modifier = Modifier.fillMaxWidth().height(22.dp),
                            contentPadding = PaddingValues(0.dp),
                            shape = RoundedCornerShape(8.dp)
                        ) { Text("▼ 下", fontSize = 11.sp, fontWeight = FontWeight.Bold) }
                    }

                    Button(
                        onClick = {
                            if (!didNextLongPress && !uiState.isPlaying) {
                                viewModel.moveCurrentBox(moveStepTap, 0, uiState.videoWidth, uiState.videoHeight)
                                viewModel.viewModelScope.launch {
                                    withContext(Dispatchers.IO) {
                                        saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                                    }
                                }
                            }
                        },
                        interactionSource = nextInteractionSource,
                        enabled = !uiState.isPlaying && viewModel.trackingMap[uiState.currentFrameIndex] != null,
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer, contentColor = MaterialTheme.colorScheme.onTertiaryContainer),
                        modifier = Modifier.weight(1f).height(48.dp),
                        shape = RoundedCornerShape(12.dp)
                    ) { Text("右 ▶", fontSize = 14.sp, fontWeight = FontWeight.ExtraBold) }
                }
            } else {
                Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                    Button(
                        onClick = { if (!didPrevLongPress && !uiState.isPlaying) viewModel.updateState { it.copy(currentFrameIndex = max(0, it.currentFrameIndex - 1)) } },
                        interactionSource = prevInteractionSource,
                        enabled = (!uiState.isPlaying && uiState.currentFrameIndex > 0 && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.surfaceVariant, contentColor = MaterialTheme.colorScheme.onSurfaceVariant),
                        modifier = Modifier.weight(1f).height(36.dp),
                        shape = RoundedCornerShape(12.dp),
                        contentPadding = PaddingValues(0.dp)
                    ) { Text("◀ 戻る", fontSize = 13.sp, fontWeight = FontWeight.Bold) }

                    Button(
                        onClick = { viewModel.updateState { it.copy(isPlaying = !it.isPlaying) } },
                        enabled = (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON),
                        colors = ButtonDefaults.buttonColors(containerColor = if (uiState.isPlaying) Color(0xFFD32F2F) else MaterialTheme.colorScheme.primary),
                        modifier = Modifier.weight(1.2f).height(44.dp),
                        shape = RoundedCornerShape(12.dp)
                    ) {
                        Icon(if (uiState.isPlaying) Icons.Default.Close else Icons.Default.PlayArrow, contentDescription = null, modifier = Modifier.size(20.dp))
                        Spacer(modifier = Modifier.width(4.dp))
                        Text(if (uiState.isPlaying) "停止" else "再生", fontSize = 14.sp, fontWeight = FontWeight.ExtraBold)
                    }

                    Button(
                        onClick = { if (!didNextLongPress && !uiState.isPlaying) viewModel.updateState { it.copy(currentFrameIndex = min(viewModel.frameFiles.size - 1, it.currentFrameIndex + 1)) } },
                        interactionSource = nextInteractionSource,
                        enabled = (!uiState.isPlaying && uiState.currentFrameIndex < viewModel.frameFiles.size - 1 && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                        colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.surfaceVariant, contentColor = MaterialTheme.colorScheme.onSurfaceVariant),
                        modifier = Modifier.weight(1f).height(36.dp),
                        shape = RoundedCornerShape(12.dp),
                        contentPadding = PaddingValues(0.dp)
                    ) { Text("進む ▶", fontSize = 13.sp, fontWeight = FontWeight.Bold) }
                }
            }

            Spacer(modifier = Modifier.height(6.dp))

            Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                Button(
                    onClick = {
                        if (viewModel.cutFrames.contains(uiState.currentFrameIndex)) {
                            viewModel.cutFrames.remove(uiState.currentFrameIndex)
                            Toast.makeText(context, "カット指定を解除しました", Toast.LENGTH_SHORT).show()
                        } else {
                            viewModel.cutFrames.add(uiState.currentFrameIndex)
                            Toast.makeText(context, "このコマからカメラが瞬間移動します", Toast.LENGTH_SHORT).show()
                        }
                        // [修正] saveProjectData を IO スレッドで実行
                        viewModel.viewModelScope.launch {
                            withContext(Dispatchers.IO) {
                                saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                            }
                        }
                    },
                    enabled = (!uiState.isPlaying && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                    colors = ButtonDefaults.buttonColors(containerColor = if (viewModel.cutFrames.contains(uiState.currentFrameIndex)) Color(0xFFAB47BC) else Color(0xFF673AB7)),
                    modifier = Modifier.weight(1.2f).height(36.dp),
                    contentPadding = PaddingValues(0.dp),
                    shape = RoundedCornerShape(8.dp)
                ) { Text(if (viewModel.cutFrames.contains(uiState.currentFrameIndex)) "カット解除" else "カット指定", color = Color.White, fontSize = 11.sp, fontWeight = FontWeight.Bold, maxLines = 1) }

                OutlinedButton(
                    onClick = {
                        viewModel.trackingMap.remove(uiState.currentFrameIndex)
                        viewModel.userKeyframes.remove(uiState.currentFrameIndex)
                        viewModel.interpolatedFrames.remove(uiState.currentFrameIndex)
                        viewModel.viewModelScope.launch {
                            withContext(Dispatchers.IO) {
                                saveProjectData(context, uiState.selectedFps, viewModel.trackingMap, viewModel.userKeyframes, viewModel.cutFrames, viewModel.interpolatedFrames)
                            }
                        }
                        Toast.makeText(context, "枠を外しました", Toast.LENGTH_SHORT).show()
                    },
                    enabled = (!uiState.isPlaying && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                    modifier = Modifier.weight(1f).height(36.dp),
                    contentPadding = PaddingValues(0.dp),
                    shape = RoundedCornerShape(8.dp)
                ) { Text("1コマ外す", color = MaterialTheme.colorScheme.error, fontSize = 11.sp, fontWeight = FontWeight.Bold, maxLines = 1) }

                Button(
                    onClick = onRemoveAfterDialog,
                    enabled = (!uiState.isPlaying && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                    colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error),
                    modifier = Modifier.weight(1f).height(36.dp),
                    contentPadding = PaddingValues(0.dp),
                    shape = RoundedCornerShape(8.dp)
                ) { Text("以降外す", color = Color.White, fontSize = 11.sp, fontWeight = FontWeight.Bold, maxLines = 1) }
            }

            Spacer(modifier = Modifier.height(2.dp))
            Slider(
                value = uiState.currentFrameIndex.toFloat(),
                onValueChange = { viewModel.updateState { state -> state.copy(currentFrameIndex = it.toInt()) } },
                valueRange = 0f..(max(1, viewModel.frameFiles.size - 1).toFloat()),
                enabled = (!uiState.isPlaying && (uiState.appMode == AppMode.EDITING || uiState.appMode == AppMode.LOCK_ON)),
                colors = SliderDefaults.colors(thumbColor = MaterialTheme.colorScheme.primary, activeTrackColor = MaterialTheme.colorScheme.primary)
            )
            Text("現在のコマ: ${uiState.currentFrameIndex} / ${max(0, viewModel.frameFiles.size - 1)}", style = MaterialTheme.typography.labelSmall, color = Color.Gray, modifier = Modifier.align(Alignment.End))
        }
    }
}

@Composable
fun SettingSectionHeader(title: String, description: String? = null) {
    Column(modifier = Modifier.fillMaxWidth().background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f), RoundedCornerShape(8.dp)).padding(6.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Icon(Icons.Default.Settings, contentDescription = null, modifier = Modifier.size(14.dp), tint = MaterialTheme.colorScheme.primary)
            Spacer(modifier = Modifier.width(4.dp))
            Text(title, style = MaterialTheme.typography.titleSmall, color = MaterialTheme.colorScheme.primary, fontWeight = FontWeight.ExtraBold)
        }
        if (description != null) {
            Spacer(modifier = Modifier.height(2.dp))
            Text(description, fontSize = 10.sp, color = Color.Gray, lineHeight = 12.sp)
        }
    }
    Spacer(modifier = Modifier.height(6.dp))
}

@Composable
fun SettingsCard(uiState: OshiCamUiState, viewModel: OshiCamViewModel, onExport: () -> Unit, context: Context) {
    var showAdvanced by remember { mutableStateOf(false) }
    val uriHandler = LocalUriHandler.current

    val trackingPriorities = listOf("色・柄を優先 (軽量ReID)", "位置・予測を優先 (SORT系)")
    val ratios = listOf("スマホ画面に合わせる (フル)", "9:16 (TikTok等)", "16:9 (YouTube等)", "4:3 (レトロ)", "1:1 (正方形)")
    val trackAxes = listOf("上下左右 (通常)", "横移動のみ (縦固定)")
    val trackingModes = listOf("常に追従 (滑らか)", "構図を固定 (ダンス等)")
    val outputResolutions = listOf("720p (SNS向け・爆速)", "1080p (Full HD)", "2K (QHD)", "4K (Ultra HD)")

    ElevatedCard(
        modifier = Modifier.fillMaxWidth(),
        elevation = CardDefaults.elevatedCardElevation(defaultElevation = 4.dp),
        shape = RoundedCornerShape(12.dp)
    ) {
        Column(modifier = Modifier.padding(8.dp)) {

            Text("設定", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.Bold, modifier = Modifier.padding(bottom = 8.dp))

            SettingSectionHeader("1. 枠の幅調整 (隣の人との混同防止)", "他の人と重なって1つの枠にされてしまう場合に狭くしてください。")
            Text("${uiState.boxWidthPercent.toInt()}%", fontSize = 14.sp, fontWeight = FontWeight.Bold, modifier = Modifier.align(Alignment.End))
            Slider(value = uiState.boxWidthPercent, onValueChange = { viewModel.updateState { state -> state.copy(boxWidthPercent = it) } }, valueRange = 5f..100f)
            Spacer(modifier = Modifier.height(8.dp))

            Row(
                modifier = Modifier.fillMaxWidth().clickable { showAdvanced = !showAdvanced }.padding(vertical = 8.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text("詳細設定 (プロ向け)", fontWeight = FontWeight.Bold, color = MaterialTheme.colorScheme.primary)
                Icon(if (showAdvanced) Icons.Default.KeyboardArrowUp else Icons.Default.KeyboardArrowDown, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
            }

            if (showAdvanced) {
                SettingSectionHeader("2. 追従の優先度 (AIの探し方)", "全員が同じ衣装の場合は「位置・予測」を優先してください。")
                trackingPriorities.forEach { prio ->
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().height(36.dp).clickable { viewModel.updateState { it.copy(selectedTrackingPriority = prio) } }) {
                        RadioButton(selected = (uiState.selectedTrackingPriority == prio), onClick = { viewModel.updateState { it.copy(selectedTrackingPriority = prio) } }, modifier = Modifier.scale(0.8f))
                        Text(prio, fontSize = 13.sp)
                    }
                }
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("3. 手動枠の大きさ", "手動でタップした際に追加される枠の基準サイズです。")
                Text("${uiState.manualBoxSizePercent.toInt()}%", fontSize = 14.sp, fontWeight = FontWeight.Bold, modifier = Modifier.align(Alignment.End))
                Slider(value = uiState.manualBoxSizePercent, onValueChange = { viewModel.updateState { state -> state.copy(manualBoxSizePercent = it) } }, valueRange = 5f..30f)
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("4. 切り抜きのサイズ (アスペクト比)")
                ratios.forEach { ratio ->
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().height(36.dp).clickable { viewModel.updateState { it.copy(selectedRatio = ratio) } }) {
                        RadioButton(selected = (uiState.selectedRatio == ratio), onClick = { viewModel.updateState { it.copy(selectedRatio = ratio) } }, modifier = Modifier.scale(0.8f))
                        Text(ratio, fontSize = 13.sp)
                    }
                }
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("5. カメラの動かし方")
                trackAxes.forEach { axis ->
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().height(36.dp).clickable { viewModel.updateState { it.copy(selectedTrackAxis = axis) } }) {
                        RadioButton(selected = (uiState.selectedTrackAxis == axis), onClick = { viewModel.updateState { it.copy(selectedTrackAxis = axis) } }, modifier = Modifier.scale(0.8f))
                        Text(axis, fontSize = 13.sp)
                    }
                }
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("6. 追従のスタイル")
                trackingModes.forEach { mode ->
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().height(36.dp).clickable { viewModel.updateState { it.copy(selectedTrackingMode = mode) } }) {
                        RadioButton(selected = (uiState.selectedTrackingMode == mode), onClick = { viewModel.updateState { it.copy(selectedTrackingMode = mode) } }, modifier = Modifier.scale(0.8f))
                        Text(mode, fontSize = 13.sp)
                    }
                }
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("7. ズーム倍率 (寄り具合)")
                Text("${uiState.cropZoomPercent.toInt()}%", fontSize = 14.sp, fontWeight = FontWeight.Bold, modifier = Modifier.align(Alignment.End))
                Slider(value = uiState.cropZoomPercent, onValueChange = { viewModel.updateState { state -> state.copy(cropZoomPercent = it) } }, valueRange = 20f..100f)
                Spacer(modifier = Modifier.height(8.dp))

                val shakeLabel = if (uiState.selectedTrackingMode == "常に追従 (滑らか)") "滑らかさの強さ" else "固定の強さ"
                SettingSectionHeader("8. 手ブレ補正 ($shakeLabel)")
                Text("${uiState.shakeReduction.toInt()}%", fontSize = 14.sp, fontWeight = FontWeight.Bold, modifier = Modifier.align(Alignment.End))
                Slider(value = uiState.shakeReduction, onValueChange = { viewModel.updateState { state -> state.copy(shakeReduction = it) } }, valueRange = 0f..100f)
                Spacer(modifier = Modifier.height(8.dp))

                SettingSectionHeader("9. 出力画質")
                outputResolutions.forEach { res ->
                    val isHighRes = res != "720p (SNS向け・爆速)"
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth().height(36.dp).clickable { viewModel.updateState { it.copy(selectedResolution = res) } }) {
                        RadioButton(selected = (uiState.selectedResolution == res), onClick = { viewModel.updateState { it.copy(selectedResolution = res) } }, modifier = Modifier.scale(0.8f))
                        Text(res, color = MaterialTheme.colorScheme.onSurface, fontSize = 13.sp)
                        if (!uiState.isPremiumVersion && isHighRes) {
                            Text(" (5秒お試し)", fontSize = 10.sp, color = Color(0xFFF57F17), fontWeight = FontWeight.Bold, modifier = Modifier.padding(start = 4.dp))
                        }
                    }
                }

                Divider(modifier = Modifier.padding(vertical = 12.dp), color = MaterialTheme.colorScheme.surfaceVariant)
            }

            ElevatedCard(
                modifier = Modifier.fillMaxWidth(),
                colors = CardDefaults.elevatedCardColors(containerColor = Color(0xFFFFF8E1)),
                elevation = CardDefaults.elevatedCardElevation(defaultElevation = 2.dp)
            ) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.fillMaxWidth()) {
                        Text("プレミアム設定", style = MaterialTheme.typography.titleSmall, color = Color(0xFFF57F17), fontWeight = FontWeight.ExtraBold)
                        Spacer(modifier = Modifier.width(8.dp))
                        if (uiState.isPremiumVersion) {
                            Badge(containerColor = Color(0xFF2E7D32)) { Text("有効", color = Color.White) }
                        }
                    }
                    if (!uiState.isPremiumVersion) {
                        Text("※無料版の720p出力は秒数無制限！(ロゴ・広告あり)\n※高画質出力は最初の5秒間のお試しになります。\n※有料版は全機能解放＆バックグラウンド処理対応！\n(※処理中はタスクキルしないでください)", fontSize = 11.sp, color = Color.DarkGray, modifier = Modifier.padding(vertical = 4.dp))
                        Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            val currentActivity = LocalContext.current as? Activity
                            Button(
                                onClick = {
                                    currentActivity?.let {
                                        purchasePremium(it) { success -> viewModel.updateState { state -> state.copy(isPremiumVersion = success) } }
                                    }
                                },
                                colors = ButtonDefaults.buttonColors(containerColor = Color(0xFFF57F17)),
                                modifier = Modifier.weight(1f).height(44.dp),
                                shape = RoundedCornerShape(8.dp)
                            ) { Text("定期購入へ進む", color = Color.White, fontSize = 13.sp, fontWeight = FontWeight.Bold) }

                            OutlinedButton(
                                onClick = {
                                    try {
                                        Purchases.sharedInstance.restorePurchasesWith(
                                            onSuccess = { customerInfo ->
                                                if (customerInfo.entitlements["premium"]?.isActive == true) {
                                                    viewModel.updateState { it.copy(isPremiumVersion = true) }
                                                    Toast.makeText(context, "購入状態を復元しました", Toast.LENGTH_SHORT).show()
                                                } else {
                                                    Toast.makeText(context, "有効な購入履歴がありません", Toast.LENGTH_SHORT).show()
                                                }
                                            },
                                            onError = { error -> Toast.makeText(context, "復元エラー: ${error.message}", Toast.LENGTH_SHORT).show() }
                                        )
                                    } catch (e: Exception) {
                                        Toast.makeText(context, "課金システムエラー: ${e.message}", Toast.LENGTH_SHORT).show()
                                    }
                                },
                                modifier = Modifier.weight(1f).height(44.dp),
                                shape = RoundedCornerShape(8.dp)
                            ) { Text("購入の復元", color = Color(0xFFF57F17), fontSize = 13.sp, fontWeight = FontWeight.Bold) }
                        }

                        // [修正] 利用規約・プライバシーポリシーのURLを正しいものに必ず差し替えてください
                        Row(modifier = Modifier.fillMaxWidth().padding(top = 8.dp), horizontalArrangement = Arrangement.End) {
                            Text("利用規約", fontSize = 11.sp, color = MaterialTheme.colorScheme.primary, textDecoration = TextDecoration.Underline, modifier = Modifier.clickable {
                                uriHandler.openUri("https://sites.google.com/view/oshicam100/%E3%83%9B%E3%83%BC%E3%83%A0")
                            })
                            Spacer(modifier = Modifier.width(12.dp))
                            Text("プライバシーポリシー", fontSize = 11.sp, color = MaterialTheme.colorScheme.primary, textDecoration = TextDecoration.Underline, modifier = Modifier.clickable {
                                uriHandler.openUri("https://sites.google.com/view/oshicam100/%E3%83%9B%E3%83%BC%E3%83%A0")
                            })
                        }
                    } else {
                        Text("プレミアム機能が有効です(フル出力/高画質/広告なし/バックグラウンド処理)\n(※処理中はタスクキルしないでください)", fontSize = 11.sp, color = Color(0xFF2E7D32), fontWeight = FontWeight.Bold, modifier = Modifier.padding(top = 4.dp))
                    }
                }
            }

            Spacer(modifier = Modifier.height(12.dp))

            if (uiState.appMode == AppMode.EDITING) {
                Button(
                    onClick = onExport,
                    modifier = Modifier.fillMaxWidth().height(48.dp),
                    enabled = !uiState.isPlaying && !uiState.isBackgroundExtracting,
                    shape = RoundedCornerShape(12.dp),
                    elevation = ButtonDefaults.buttonElevation(defaultElevation = 4.dp)
                ) {
                    Text(
                        text = if (uiState.isBackgroundExtracting) "展開完了までお待ちください..." else "音付きで動画を書き出す",
                        color = MaterialTheme.colorScheme.onPrimary,
                        fontSize = 15.sp,
                        fontWeight = FontWeight.ExtraBold
                    )
                }
            }
        }
    }
}

// --- 以下、バックグラウンド処理系関数 ---

fun getFolderSizeBytes(file: File): Long {
    var size = 0L
    if (file.isDirectory) { file.listFiles()?.forEach { child -> size += getFolderSizeBytes(child) } }
    else { size = file.length() }
    return size
}

suspend fun saveToSlot(context: Context, slot: Int): Boolean = withContext(Dispatchers.IO) {
    try {
        val finalDir = File(context.filesDir, "slot_$slot")
        val tempDir = File(context.filesDir, "slot_${slot}_temp")
        if (tempDir.exists()) tempDir.deleteRecursively()
        tempDir.mkdirs()

        val projFile = File(context.filesDir, "project_data.txt")
        if (projFile.exists()) projFile.copyTo(File(tempDir, "project_data.txt"), true)

        val videoFile = File(context.filesDir, "working_video.mp4")
        if (videoFile.exists()) videoFile.copyTo(File(tempDir, "working_video.mp4"), true)

        val framesDir = File(context.filesDir, "extracted_frames")
        if (framesDir.exists()) framesDir.copyRecursively(File(tempDir, "extracted_frames"), true)

        if (finalDir.exists()) finalDir.deleteRecursively()
        tempDir.renameTo(finalDir)
        true
    } catch (e: Exception) {
        e.printStackTrace()
        false
    }
}

suspend fun loadFromSlot(context: Context, slot: Int): Boolean = withContext(Dispatchers.IO) {
    try {
        val slotDir = File(context.filesDir, "slot_$slot")
        if (!slotDir.exists()) return@withContext false

        File(context.filesDir, "working_video.mp4").delete()
        File(context.filesDir, "project_data.txt").delete()
        File(context.filesDir, "extracted_frames").deleteRecursively()

        val slotProj = File(slotDir, "project_data.txt")
        if (slotProj.exists()) slotProj.copyTo(File(context.filesDir, "project_data.txt"), true)

        val slotVideo = File(slotDir, "working_video.mp4")
        if (slotVideo.exists()) slotVideo.copyTo(File(context.filesDir, "working_video.mp4"), true)

        val slotFrames = File(slotDir, "extracted_frames")
        if (slotFrames.exists()) slotFrames.copyRecursively(File(context.filesDir, "extracted_frames"), true)
        true
    } catch (e: Exception) {
        e.printStackTrace()
        false
    }
}

// [修正] saveProjectData / loadProjectData を通常関数のままにし、呼び出し側で withContext(Dispatchers.IO) を使用
fun saveProjectData(context: Context, fps: Int, trackingMap: Map<Int, Rect>, userKeyframes: List<Int>, cutFrames: List<Int>, interpolatedFrames: List<Int>) {
    try {
        val file = File(context.filesDir, "project_data.txt")
        val sb = java.lang.StringBuilder()
        sb.append("CONFIG,$fps\n")
        for ((index, rect) in trackingMap) {
            val isKey = if (userKeyframes.contains(index)) 1 else 0
            val isCut = if (cutFrames.contains(index)) 1 else 0
            val isInterp = if (interpolatedFrames.contains(index)) 1 else 0
            sb.append("FRAME,$index,${rect.left},${rect.top},${rect.right},${rect.bottom},$isKey,$isCut,$isInterp\n")
        }
        file.writeText(sb.toString())
    } catch (e: Exception) { e.printStackTrace() }
}

fun loadProjectData(context: Context, trackingMap: MutableMap<Int, Rect>, userKeyframes: MutableList<Int>, cutFrames: MutableList<Int>, interpolatedFrames: MutableList<Int>): Int {
    var fps = 15
    try {
        val file = File(context.filesDir, "project_data.txt")
        if (!file.exists()) return fps
        trackingMap.clear(); userKeyframes.clear(); cutFrames.clear(); interpolatedFrames.clear()

        file.readLines().forEach { line ->
            val parts = line.split(",")
            if (parts[0] == "CONFIG" && parts.size >= 2) {
                fps = parts[1].toIntOrNull() ?: 15
            } else if (parts[0] == "FRAME" && parts.size >= 7) {
                runCatching {
                    val index = parts[1].toInt()
                    val rect = Rect(parts[2].toInt(), parts[3].toInt(), parts[4].toInt(), parts[5].toInt())
                    trackingMap[index] = rect
                    if (parts[6].toIntOrNull() == 1) userKeyframes.add(index)
                    if (parts.getOrNull(7)?.toIntOrNull() == 1) cutFrames.add(index)
                    if (parts.getOrNull(8)?.toIntOrNull() == 1) interpolatedFrames.add(index)
                }.onFailure { e ->
                    Log.w("ProjectData", "行の読み込みスキップ: $line", e)
                }
            }
        }
    } catch(e: Exception) { e.printStackTrace() }
    return fps
}

suspend fun copyUriToFile(context: Context, uri: Uri): File = withContext(Dispatchers.IO) {
    val dest = File(context.filesDir, "working_video.mp4")
    context.contentResolver.openInputStream(uri)?.use { input ->
        FileOutputStream(dest).buffered(8 * 1024 * 1024).use { output ->
            input.copyTo(output, bufferSize = 8 * 1024 * 1024)
        }
    }
    dest
}

suspend fun runAutoTracking(
    startStep: Int,
    startBox: Rect,
    framesDir: File,
    estimatedTotal: Int,
    isBgExtracting: () -> Boolean,
    allFramesObjectsCopy: Map<Int, List<Rect>>,
    trackingPriority: String,
    boxWidthPercent: Float,
    pauseOnLostTracker: Boolean,
    onProgress: suspend (String) -> Unit,
    onFrameUpdate: suspend (Int, Rect?, List<Rect>?, Bitmap?) -> Unit,
    isCancelled: () -> Boolean
): Int? = withContext(Dispatchers.Default) {
    var objDetector: ObjectDetector? = null
    var faceDetector: FaceDetector? = null
    var returnedLostFrame: Int? = null

    try {
        val objOptions = ObjectDetectorOptions.Builder().setDetectorMode(ObjectDetectorOptions.SINGLE_IMAGE_MODE).enableMultipleObjects().build()
        objDetector = ObjectDetection.getClient(objOptions)

        val faceOptions = FaceDetectorOptions.Builder().setPerformanceMode(FaceDetectorOptions.PERFORMANCE_MODE_FAST).build()
        faceDetector = FaceDetection.getClient(faceOptions)

        var startBmp: Bitmap? = null
        try {
            val initialFrames = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()
            if (startStep < initialFrames.size) {
                val options = BitmapFactory.Options().apply { inSampleSize = 2; inPreferredConfig = Bitmap.Config.RGB_565 }
                startBmp = BitmapFactory.decodeFile(initialFrames[startStep].absolutePath, options)
            }
        } catch (e: Exception) { Log.e("AutoTracking", "開始フレーム読み込みエラー", e) }

        if (startBmp == null) return@withContext null

        val scaleFactor = 2f
        val scaledStartBox = Rect((startBox.left / scaleFactor).toInt(), (startBox.top / scaleFactor).toInt(), (startBox.right / scaleFactor).toInt(), (startBox.bottom / scaleFactor).toInt())

        val masterEmbedding = getSpatialEmbedding(startBmp, scaledStartBox)
        var lastKnownEmbedding = masterEmbedding.copyOf()

        var lastBox = startBox
        startBmp.recycle()

        val isDistPriority = trackingPriority.contains("距離") || trackingPriority.contains("位置") || trackingPriority.contains("予測")

        var step = startStep + 1
        var velocityX = 0f
        var velocityY = 0f
        var notFoundCount = 0

        val threshold = if (isDistPriority) 0.40f else 0.50f
        val updateThreshold = if (isDistPriority) 0.65f else 0.75f

        while (isActive && !isCancelled()) {
            var currentFrames = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()

            var retryCount = 0
            while (step >= currentFrames.size && isBgExtracting()) {
                if (!isActive || isCancelled()) break
                delay(300)
                retryCount++
                if (retryCount > 200) break
                currentFrames = framesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()
            }

            if (step >= currentFrames.size || isCancelled()) break

            val modeName = if (isDistPriority) "SORT系(予測優先)" else "軽量ReID(柄優先)"
            val currentTotal = max(estimatedTotal, currentFrames.size)
            val percent = if (currentTotal > 0) ((step.toFloat() / currentTotal) * 100).toInt().coerceIn(0, 100) else 0

            withContext(Dispatchers.Main) { onProgress("AI追従中($modeName)... $percent% ($step コマ)") }

            var bitmap: Bitmap? = null
            try {
                val processOptions = BitmapFactory.Options().apply { inSampleSize = 2; inPreferredConfig = Bitmap.Config.RGB_565 }
                bitmap = BitmapFactory.decodeFile(currentFrames[step].absolutePath, processOptions)
                if (bitmap == null) {
                    delay(200)
                    bitmap = BitmapFactory.decodeFile(currentFrames[step].absolutePath, processOptions)
                    if (bitmap == null) { step++; continue }
                }

                val imgW = (bitmap.width * scaleFactor).toInt()
                val imgH = (bitmap.height * scaleFactor).toInt()

                val image = InputImage.fromBitmap(bitmap, 0)

                val rawObjs = detectObjectsSync(objDetector, image)
                val rawFaces = detectFacesSync(faceDetector, image)

                // [修正] 共通関数 mergeDetectionBoxes を使用（重複コード廃止）
                val newFilteredObjs = mergeDetectionBoxes(
                    rawObjs, rawFaces, scaleFactor, imgW, imgH, boxWidthPercent, lastBox
                )

                val predX = lastBox.centerX().toFloat() + velocityX
                val predY = lastBox.centerY().toFloat() + velocityY

                var bestB: Rect? = null
                var bestS = 0f
                var bestEmbedding = lastKnownEmbedding

                for (box in newFilteredObjs) {
                    if (box.width() <= 0 || box.height() <= 0) continue

                    val evalBox = Rect((box.left / scaleFactor).toInt(), (box.top / scaleFactor).toInt(), (box.right / scaleFactor).toInt(), (box.bottom / scaleFactor).toInt())

                    val currEmbedding = getSpatialEmbedding(bitmap, evalBox)
                    val score = calculateReIDScore(lastBox, box, currEmbedding, lastKnownEmbedding, masterEmbedding, predX, py = predY, trackingPriority)

                    if (score > bestS) {
                        bestS = score
                        bestB = box
                        bestEmbedding = currEmbedding
                    }
                }

                var isFound = false
                if (bestB != null && bestS > threshold) {
                    velocityX = velocityX * 0.5f + (bestB.centerX() - lastBox.centerX()) * 0.5f
                    velocityY = velocityY * 0.5f + (bestB.centerY() - lastBox.centerY()) * 0.5f

                    val lastCenterX = lastBox.centerX().toFloat()
                    val lastCenterY = lastBox.centerY().toFloat()
                    val lastWidth = lastBox.width().toFloat()
                    val lastHeight = lastBox.height().toFloat()

                    val targetCenterX = bestB.centerX().toFloat()
                    val targetCenterY = bestB.centerY().toFloat()
                    val targetWidth = bestB.width().toFloat()
                    val targetHeight = bestB.height().toFloat()

                    val newCenterX = (lastCenterX * 0.4f + targetCenterX * 0.6f).toInt()
                    val newCenterY = (lastCenterY * 0.4f + targetCenterY * 0.6f).toInt()
                    val newWidth = (lastWidth * 0.85f + targetWidth * 0.15f).toInt()
                    val newHeight = (lastHeight * 0.85f + targetHeight * 0.15f).toInt()

                    val smoothedLeft = newCenterX - newWidth / 2
                    val smoothedTop = newCenterY - newHeight / 2
                    val smoothedRight = newCenterX + newWidth / 2
                    val smoothedBottom = newCenterY + newHeight / 2

                    lastBox = Rect(smoothedLeft, smoothedTop, smoothedRight, smoothedBottom)

                    if (bestS > updateThreshold) lastKnownEmbedding = updateEmbedding(lastKnownEmbedding, bestEmbedding)
                    isFound = true
                    notFoundCount = 0
                } else {
                    val shiftX = velocityX.toInt()
                    val shiftY = velocityY.toInt()
                    lastBox = Rect(
                        max(0, lastBox.left + shiftX), max(0, lastBox.top + shiftY),
                        min(imgW, lastBox.right + shiftX), min(imgH, lastBox.bottom + shiftY)
                    )

                    velocityX *= 0.6f
                    velocityY *= 0.6f

                    isFound = false
                    notFoundCount++

                    if (pauseOnLostTracker && notFoundCount >= 10) {
                        returnedLostFrame = step - notFoundCount + 1
                        break
                    }
                }

                var previewBmp: Bitmap? = null
                if (step % 5 == 0) {
                    try {
                        val uiOptions = BitmapFactory.Options().apply { inSampleSize = 4; inPreferredConfig = Bitmap.Config.RGB_565 }
                        previewBmp = BitmapFactory.decodeFile(currentFrames[step].absolutePath, uiOptions)
                    } catch (e: Exception) { Log.e("AutoTracking", "プレビュー読み込みエラー", e) }
                }

                withContext(Dispatchers.Main) { onFrameUpdate(step, if (isFound) lastBox else null, newFilteredObjs, previewBmp) }
                delay(5)
            } finally {
                bitmap?.recycle()
            }
            step++
        }
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        Log.e("AutoTracking", "追従中にエラーが発生", e)
    } finally {
        objDetector?.close()
        faceDetector?.close()
    }

    return@withContext returnedLostFrame
}

fun getSpatialEmbedding(bitmap: Bitmap, bounds: Rect): FloatArray {
    val b = Rect(
        max(0, bounds.left), max(0, bounds.top),
        min(bitmap.width, bounds.right), min(bitmap.height, bounds.bottom)
    )

    val embedding = FloatArray(75)
    if (b.width() <= 0 || b.height() <= 0) return embedding

    val cropped = Bitmap.createBitmap(bitmap, b.left, b.top, b.width(), b.height())
    val scaled = Bitmap.createScaledBitmap(cropped, 15, 15, true)

    val hsv = FloatArray(3)
    var index = 0

    for (row in 0 until 5) {
        for (col in 0 until 5) {
            var sumCosHS = 0f; var sumSinHS = 0f; var sumV = 0f
            for (y in 0 until 3) {
                for (x in 0 until 3) {
                    val pixel = scaled.getPixel(col * 3 + x, row * 3 + y)
                    android.graphics.Color.colorToHSV(pixel, hsv)

                    val hueRad = Math.toRadians(hsv[0].toDouble())
                    val s = hsv[1]
                    val v = hsv[2]

                    sumCosHS += (Math.cos(hueRad).toFloat() * s)
                    sumSinHS += (Math.sin(hueRad).toFloat() * s)
                    sumV += v
                }
            }
            val avgCosHS = sumCosHS / 9f
            val avgSinHS = sumSinHS / 9f
            val avgV = sumV / 9f

            embedding[index++] = avgCosHS
            embedding[index++] = avgSinHS
            embedding[index++] = avgV
        }
    }

    cropped.recycle()
    scaled.recycle()
    return embedding
}

val CENTER_WEIGHTS = floatArrayOf(
    0.1f, 0.2f, 0.2f, 0.2f, 0.1f,
    0.2f, 0.8f, 1.0f, 0.8f, 0.2f,
    0.2f, 1.0f, 1.5f, 1.0f, 0.2f,
    0.2f, 0.8f, 1.0f, 0.8f, 0.2f,
    0.1f, 0.2f, 0.2f, 0.2f, 0.1f
)

fun cosineSimilarity(vec1: FloatArray, vec2: FloatArray): Float {
    var dotProduct = 0f
    var normA = 0f
    var normB = 0f
    for (i in 0 until 25) {
        val w = CENTER_WEIGHTS[i]
        val baseIdx = i * 3

        for(j in 0 until 3) {
            val v1 = vec1[baseIdx + j]
            val v2 = vec2[baseIdx + j]
            dotProduct += (v1 * v2) * w
            normA += (v1 * v1) * w
            normB += (v2 * v2) * w
        }
    }
    if (normA == 0f || normB == 0f) return 0f
    return dotProduct / (sqrt(normA.toDouble()) * sqrt(normB.toDouble())).toFloat()
}

fun calculateReIDScore(last: Rect, curr: Rect, currEmbedding: FloatArray, lastEmbedding: FloatArray, masterEmbedding: FloatArray, px: Float, py: Float, trackingPriority: String): Float {
    val sizeRatioW = curr.width().toFloat() / max(1f, last.width().toFloat())
    val sizeRatioH = curr.height().toFloat() / max(1f, last.height().toFloat())

    val maxScale = 3.0f
    val minScale = 0.3f

    if (sizeRatioW > maxScale || sizeRatioW < minScale) return 0f
    if (sizeRatioH > maxScale || sizeRatioH < minScale) return 0f

    val distX = abs(curr.centerX() - px)
    val distY = abs(curr.centerY() - py)
    val isDistPriority = trackingPriority.contains("予測") || trackingPriority.contains("位置") || trackingPriority.contains("距離")

    val maxAllowedDistX = last.width() * if (isDistPriority) 1.0f else 1.5f
    val maxAllowedDistY = last.height() * if (isDistPriority) 0.8f else 1.2f

    if (distX > maxAllowedDistX || distY > maxAllowedDistY) {
        return 0f
    }

    val scoreL = cosineSimilarity(currEmbedding, lastEmbedding)
    val scoreM = cosineSimilarity(currEmbedding, masterEmbedding)

    val minL = 0.40f
    val minM = 0.35f

    if (scoreL < minL || scoreM < minM) return 0f

    val visualScore = scoreL * 0.4f + scoreM * 0.6f

    val distScoreX = max(0f, 1f - (distX / maxAllowedDistX))
    val distScoreY = max(0f, 1f - (distY / maxAllowedDistY))
    val distScore = (distScoreX * distScoreY)

    return if (isDistPriority) {
        visualScore * 0.2f + distScore * 0.8f
    } else {
        visualScore * 0.6f + distScore * 0.4f
    }
}

fun updateEmbedding(old: FloatArray, new: FloatArray): FloatArray {
    val res = FloatArray(75)
    for (i in 0 until 75) {
        res[i] = old[i] * 0.85f + new[i] * 0.15f
    }
    return res
}

suspend fun detectObjectsSync(detector: ObjectDetector, image: InputImage): List<Rect> =
    kotlinx.coroutines.withTimeoutOrNull(3000) {
        suspendCancellableCoroutine { cont ->
            try {
                detector.process(image)
                    .addOnSuccessListener { if (cont.isActive) cont.resume(it.map { o -> o.boundingBox }) }
                    .addOnFailureListener { if (cont.isActive) cont.resume(emptyList()) }
            } catch (e: Exception) { if (cont.isActive) cont.resume(emptyList()) }
        }
    } ?: emptyList()

suspend fun detectFacesSync(detector: FaceDetector, image: InputImage): List<Rect> =
    kotlinx.coroutines.withTimeoutOrNull(3000) {
        suspendCancellableCoroutine { cont ->
            try {
                detector.process(image)
                    .addOnSuccessListener { if (cont.isActive) cont.resume(it.map { f -> f.boundingBox }) }
                    .addOnFailureListener { if (cont.isActive) cont.resume(emptyList()) }
            } catch (e: Exception) { if (cont.isActive) cont.resume(emptyList()) }
        }
    } ?: emptyList()

suspend fun processVideoProfessional(
    context: Context, originalVideo: File, frameFiles: List<File>, trackingMap: Map<Int, Rect>,
    ratio: String, zoom: Float, selectedFps: Int, isSmoothExport: Boolean, trackAxis: String, trackingMode: String,
    resolutionSetting: String, shakeReduction: Float, cutFrames: List<Int>,
    isPremium: Boolean,
    onProgress: (String) -> Unit,
    contextForToast: Context,
    viewModel: OshiCamViewModel,
    isCancelled: () -> Boolean
): File? = withContext(Dispatchers.IO) {

    try {
        if (isPremium) {
            VideoProcessingService.start(context, "書き出しの準備中...")
        }

        val startTimeMs = System.currentTimeMillis()
        val exportFps = if (isSmoothExport && selectedFps < 30) 30 else selectedFps

        val p1Weight = 20
        val p2Weight = 50
        val p3Weight = 30
        val p1Base = 0
        val p2Base = p1Weight
        val p3Base = p1Weight + p2Weight

        var smoothedRemainingMs = -1L

        val reportProgress = { phaseMsg: String, currentPhasePercent: Float, basePercent: Int, phaseWeight: Int ->
            val overallPercentFloat = basePercent.toFloat() + (currentPhasePercent * (phaseWeight / 100f)).coerceIn(0f, phaseWeight.toFloat())
            val overallPercentInt = overallPercentFloat.toInt()

            val elapsedMs = System.currentTimeMillis() - startTimeMs
            val etaStr = if (overallPercentFloat > 0.5f) {
                val totalEstimatedMs = (elapsedMs * 100f / overallPercentFloat).toLong()
                val rawRemainingMs = max(0L, totalEstimatedMs - elapsedMs)

                if (smoothedRemainingMs == -1L) {
                    smoothedRemainingMs = rawRemainingMs
                } else {
                    smoothedRemainingMs = (smoothedRemainingMs * 0.8f + rawRemainingMs * 0.2f).toLong()
                }

                val remainingSec = (smoothedRemainingMs / 1000) % 60
                val remainingMin = (smoothedRemainingMs / 1000) / 60
                if (smoothedRemainingMs > 1000) "残り約 ${remainingMin}分 ${remainingSec}秒" else "まもなく完了..."
            } else {
                "残り時間計算中..."
            }
            val fullMessage = "$phaseMsg\n全体進捗: $overallPercentInt%\n$etaStr"
            onProgress(fullMessage)

            if (isPremium) {
                VideoProcessingService.update(context, phaseMsg, overallPercentInt)
            }
        }

        val exportFramesDir = File(context.cacheDir, "export_frames")
        if (!exportFramesDir.exists()) exportFramesDir.mkdirs()
        exportFramesDir.listFiles()?.forEach { it.deleteRecursively() }

        reportProgress("準備中 (最高画質データ生成)", 0f, p1Base, p1Weight)

        val isHighRes = resolutionSetting != "720p (SNS向け・爆速)"
        val timeLimitOption = if (!isPremium && isHighRes) "-t 5" else ""

        val scaleFilter = "-vf fps=$exportFps"

        val command = if (timeLimitOption.isNotEmpty()) {
            "-y -threads 0 -i '${originalVideo.absolutePath}' $timeLimitOption $scaleFilter -qscale:v 1 '${exportFramesDir.absolutePath}/frame_%05d.jpg'"
        } else {
            "-y -threads 0 -i '${originalVideo.absolutePath}' $scaleFilter -qscale:v 1 '${exportFramesDir.absolutePath}/frame_%05d.jpg'"
        }

        val successPhase1 = suspendCancellableCoroutine<Boolean> { cont ->
            val session = FFmpegKit.executeAsync(command,
                { session ->
                    if (cont.isActive) {
                        if (isCancelled() || session.returnCode.isValueCancel) {
                            cont.resume(false)
                        } else {
                            cont.resume(session.returnCode.isValueSuccess)
                        }
                    }
                },
                { _ -> },
                { statistics ->
                    val expectedFrames = if (!isPremium && isHighRes) {
                        5 * exportFps.toFloat()
                    } else {
                        frameFiles.size * (exportFps.toFloat() / selectedFps.toFloat())
                    }
                    if (expectedFrames > 0) {
                        val percent = (statistics.videoFrameNumber.toFloat() / expectedFrames * 100f).coerceIn(0f, 100f)
                        reportProgress("準備中 (最高画質データ生成)", percent, p1Base, p1Weight)
                    }
                }
            )
            viewModel.activeFfmpegSessionId = session.sessionId
        }
        viewModel.activeFfmpegSessionId = null

        if (isCancelled()) return@withContext null
        if (!successPhase1) throw Exception("補間データの生成に失敗しました")

        val fullExportFrameFiles = exportFramesDir.listFiles()?.filter { it.extension == "jpg" }?.sortedBy { it.name } ?: emptyList()
        if (fullExportFrameFiles.isEmpty()) throw Exception("出力用フレームが見つかりません")

        val maxAllowedFrames = if (!isPremium && isHighRes) 5 * exportFps else fullExportFrameFiles.size
        val exportFrameFiles = fullExportFrameFiles.take(maxAllowedFrames)

        val editOptions = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeFile(frameFiles[0].absolutePath, editOptions)
        val editW = editOptions.outWidth.toFloat()
        val editH = editOptions.outHeight.toFloat()

        val expOptions = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        BitmapFactory.decodeFile(exportFrameFiles[0].absolutePath, expOptions)
        val expW = expOptions.outWidth.toFloat()
        val expH = expOptions.outHeight.toFloat()

        val scaleX = expW / editW
        val scaleY = expH / editH

        val targetRatio = when (ratio) {
            "スマホ画面に合わせる (フル)" -> {
                val metrics = context.resources.displayMetrics
                val w = min(metrics.widthPixels, metrics.heightPixels).toFloat()
                val h = max(metrics.widthPixels, metrics.heightPixels).toFloat()
                w / h
            }
            "9:16 (TikTok等)" -> 9f / 16f; "16:9 (YouTube等)" -> 16f / 9f; "4:3 (レトロ)" -> 4f / 3f; "1:1 (正方形)" -> 1f; else -> 9f / 16f
        }

        var cropW: Int; var cropH: Int
        if (expW / expH > targetRatio) {
            cropH = (expH * (zoom / 100f)).toInt()
            cropW = (cropH * targetRatio).toInt()
        } else {
            cropW = (expW * (zoom / 100f)).toInt()
            cropH = (cropW / targetRatio).toInt()
        }
        cropW = max(2, (cropW / 2) * 2)
        cropH = max(2, (cropH / 2) * 2)

        val targetLongestEdge = when (resolutionSetting) {
            "4K (Ultra HD)" -> 3840
            "2K (QHD)" -> 2560
            "1080p (Full HD)" -> 1920
            else -> 1280
        }

        var outW: Int; var outH: Int
        if (cropW >= cropH) {
            outW = targetLongestEdge
            outH = (outW * (cropH.toFloat() / cropW.toFloat())).toInt()
        } else {
            outH = targetLongestEdge
            outW = (outH * (cropW.toFloat() / cropH.toFloat())).toInt()
        }
        outW = max(2, (outW / 2) * 2)
        outH = max(2, (outH / 2) * 2)

        val croppedDir = File(context.cacheDir, "cropped_frames")
        if (!croppedDir.exists()) croppedDir.mkdirs()
        croppedDir.listFiles()?.forEach { it.deleteRecursively() }

        val editCamX = FloatArray(frameFiles.size)
        val editCamY = FloatArray(frameFiles.size)

        val smoothingBase = 1.0f - (shakeReduction / 100f) * 0.95f
        val isDanceMode = (trackingMode == "構図を固定 (ダンス等)")

        var curCamX = (trackingMap[0]?.centerX()?.toFloat() ?: (editW / 2f)) * scaleX
        var curCamY = if (trackAxis == "横移動のみ (縦固定)") (expH / 2f) else ((trackingMap[0]?.centerY()?.toFloat() ?: (editH / 2f)) * scaleY)

        for (i in frameFiles.indices) {
            val box = trackingMap[i]
            if (box != null) {
                val targetX = box.centerX().toFloat() * scaleX
                val targetY = box.centerY().toFloat() * scaleY

                if (cutFrames.contains(i)) {
                    curCamX = targetX; if (trackAxis == "上下左右 (通常)") curCamY = targetY
                } else {
                    val distX = abs(targetX - curCamX)
                    val distY = abs(targetY - curCamY)

                    val speedFactorX = if (isDanceMode) {
                        if (distX < cropW * 0.1f) 0.1f else 1.5f
                    } else {
                        1.0f
                    }
                    val speedFactorY = if (isDanceMode) {
                        if (distY < cropH * 0.1f) 0.1f else 1.5f
                    } else {
                        1.0f
                    }

                    curCamX += (targetX - curCamX) * (smoothingBase * speedFactorX).coerceIn(0.01f, 1f)
                    if (trackAxis == "上下左右 (通常)") {
                        curCamY += (targetY - curCamY) * (smoothingBase * speedFactorY).coerceIn(0.01f, 1f)
                    } else {
                        curCamY = expH / 2f
                    }
                }
            }
            editCamX[i] = curCamX
            editCamY[i] = curCamY
        }

        val fpsRatioFloat = selectedFps.toFloat() / exportFps.toFloat()

        var completedFrames = 0

        val maxMemMB = Runtime.getRuntime().maxMemory() / (1024 * 1024)
        val chunkSize = when {
            resolutionSetting == "4K (Ultra HD)" -> 1
            maxMemMB > 800 -> 3
            maxMemMB > 400 -> 2
            else -> 1
        }

        for (chunk in exportFrameFiles.indices.chunked(chunkSize)) {
            if (!isActive || isCancelled()) {
                System.gc()
                return@withContext null
            }
            withContext(Dispatchers.Default) {
                chunk.map { j ->
                    async {
                        val editIdxFloat = j * fpsRatioFloat
                        val idx1 = editIdxFloat.toInt().coerceIn(0, frameFiles.size - 1)
                        val idx2 = (idx1 + 1).coerceIn(0, frameFiles.size - 1)
                        val weight = editIdxFloat - idx1

                        var finalCamX = editCamX[idx1]
                        var finalCamY = editCamY[idx1]

                        if (idx1 != idx2) {
                            if (cutFrames.contains(idx2)) {
                                if (weight >= 0.5f) {
                                    finalCamX = editCamX[idx2]; finalCamY = editCamY[idx2]
                                }
                            } else {
                                finalCamX = editCamX[idx1] * (1 - weight) + editCamX[idx2] * weight
                                finalCamY = editCamY[idx1] * (1 - weight) + editCamY[idx2] * weight
                            }
                        }

                        val safeX = (finalCamX - cropW / 2).toInt().coerceIn(0, max(0, expW.toInt() - cropW))
                        val safeY = (finalCamY - cropH / 2).toInt().coerceIn(0, max(0, expH.toInt() - cropH))

                        var croppedBitmap: Bitmap? = null

                        try {
                            val decoder = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                                BitmapRegionDecoder.newInstance(exportFrameFiles[j].absolutePath)
                            } else {
                                @Suppress("DEPRECATION")
                                BitmapRegionDecoder.newInstance(exportFrameFiles[j].absolutePath, false)
                            }

                            if (decoder != null) {
                                val cropRect = Rect(safeX, safeY, safeX + cropW, safeY + cropH)
                                val decodeOptions = BitmapFactory.Options().apply { inPreferredConfig = Bitmap.Config.RGB_565 }
                                croppedBitmap = decoder.decodeRegion(cropRect, decodeOptions)
                                decoder.recycle()
                            }
                        } catch (e: Exception) {
                            Log.e("Export", "BitmapRegionDecoder エラー (j=$j)", e)
                        }

                        if (croppedBitmap == null) {
                            try {
                                val decodeOptions = BitmapFactory.Options().apply {
                                    inPreferredConfig = Bitmap.Config.RGB_565
                                }
                                val fullBmp = BitmapFactory.decodeFile(exportFrameFiles[j].absolutePath, decodeOptions)

                                if (fullBmp != null) {
                                    val safeLeft = safeX.coerceIn(0, max(0, fullBmp.width - 1))
                                    val safeTop = safeY.coerceIn(0, max(0, fullBmp.height - 1))
                                    val safeCropW = min(cropW, fullBmp.width - safeLeft)
                                    val safeCropH = min(cropH, fullBmp.height - safeTop)

                                    if (safeCropW > 0 && safeCropH > 0) {
                                        croppedBitmap = Bitmap.createBitmap(fullBmp, safeLeft, safeTop, safeCropW, safeCropH)
                                    }
                                    fullBmp.recycle()
                                }
                            } catch(e: Throwable) {
                                Log.e("Export", "フルBitmapクロップ失敗 (j=$j)", e)
                            }
                        }

                        val outFile = File(croppedDir, String.format("frame_%05d.jpg", j))

                        try {
                            if (croppedBitmap != null) {
                                if (!isPremium) {
                                    val mutableBmp = croppedBitmap.copy(Bitmap.Config.RGB_565, true)
                                    val canvas = android.graphics.Canvas(mutableBmp)
                                    val paint = android.graphics.Paint().apply {
                                        color = android.graphics.Color.WHITE
                                        alpha = 180
                                        textSize = mutableBmp.width * 0.05f
                                        isAntiAlias = true
                                        setShadowLayer(3f, 1f, 1f, android.graphics.Color.BLACK)
                                    }
                                    val text = "OshiCam"
                                    val textWidth = paint.measureText(text)
                                    canvas.drawText(text, mutableBmp.width - textWidth - (mutableBmp.width * 0.05f), mutableBmp.height - (mutableBmp.height * 0.05f), paint)

                                    FileOutputStream(outFile).use { outStream -> mutableBmp.compress(Bitmap.CompressFormat.JPEG, 90, outStream) }
                                    mutableBmp.recycle()
                                } else {
                                    FileOutputStream(outFile).use { outStream -> croppedBitmap.compress(Bitmap.CompressFormat.JPEG, 90, outStream) }
                                }
                            }
                        } catch (e: Throwable) {
                            Log.e("Export", "フレーム書き込みエラー (j=$j)", e)
                        } finally {
                            croppedBitmap?.recycle()
                        }
                    }
                }.awaitAll()

                System.gc()
            }

            completedFrames += chunk.size
            val percent = (completedFrames.toFloat() / exportFrameFiles.size * 100f).coerceIn(0f, 100f)
            withContext(Dispatchers.Main) {
                reportProgress("最高画質で切り抜き中 (省メモリ処理)", percent, p2Base, p2Weight)
            }
        }

        var lastValidFile: File? = null
        for (j in exportFrameFiles.indices) {
            val file = File(croppedDir, String.format("frame_%05d.jpg", j))
            if (file.exists() && file.length() > 0) {
                lastValidFile = file
                break
            }
        }

        if (lastValidFile == null) {
            throw Exception("画像の切り抜きにすべて失敗しました。非対応の動画形式か、メモリが極端に不足している可能性があります。")
        }

        for (j in exportFrameFiles.indices) {
            val file = File(croppedDir, String.format("frame_%05d.jpg", j))
            if (!file.exists() || file.length() == 0L) {
                lastValidFile?.copyTo(file, overwrite = true)
            } else {
                lastValidFile = file
            }
        }

        reportProgress("最終動画データを生成中", 0f, p3Base, p3Weight)
        val outputFile = File(context.cacheDir, "output.mp4")

        val timeLimitOptionForEncode = if (!isPremium && isHighRes) "-t 5" else ""
        val commandFinal = if (timeLimitOptionForEncode.isNotEmpty()) {
            "-y -framerate $exportFps -start_number 0 -i '${croppedDir.absolutePath}/frame_%05d.jpg' $timeLimitOptionForEncode -i '${originalVideo.absolutePath}' -map 0:v:0 -map 1:a:0? -vf \"scale=$outW:$outH:flags=lanczos\" -colorspace bt709 -color_primaries bt709 -color_trc bt709 -c:v libx264 -crf 18 -preset fast -pix_fmt yuv420p -c:a aac -b:a 192k -ar 44100 -shortest '${outputFile.absolutePath}'"
        } else {
            "-y -framerate $exportFps -start_number 0 -i '${croppedDir.absolutePath}/frame_%05d.jpg' -i '${originalVideo.absolutePath}' -map 0:v:0 -map 1:a:0? -vf \"scale=$outW:$outH:flags=lanczos\" -colorspace bt709 -color_primaries bt709 -color_trc bt709 -c:v libx264 -crf 18 -preset fast -pix_fmt yuv420p -c:a aac -b:a 192k -ar 44100 -shortest '${outputFile.absolutePath}'"
        }

        val totalFrames = exportFrameFiles.size

        val successPhase3 = suspendCancellableCoroutine<Boolean> { cont ->
            val session = FFmpegKit.executeAsync(commandFinal,
                { session ->
                    if (cont.isActive) {
                        if (isCancelled() || session.returnCode.isValueCancel) {
                            cont.resume(false)
                        } else {
                            cont.resume(session.returnCode.isValueSuccess)
                        }
                    }
                },
                { _ -> },
                { statistics ->
                    if (totalFrames > 0) {
                        val percent = (statistics.videoFrameNumber.toFloat() / totalFrames * 100f).coerceIn(0f, 100f)
                        reportProgress("最終動画データを生成中", percent, p3Base, p3Weight)
                    }
                }
            )
            viewModel.activeFfmpegSessionId = session.sessionId
        }
        viewModel.activeFfmpegSessionId = null

        if (isCancelled()) return@withContext null
        if (!successPhase3) throw Exception("動画のエンコードに失敗しました。")

        croppedDir.listFiles()?.forEach { it.deleteRecursively() }
        exportFramesDir.listFiles()?.forEach { it.deleteRecursively() }
        outputFile

    } catch (e: Throwable) {
        System.gc()
        if (!isCancelled()) {
            withContext(Dispatchers.Main) {
                Toast.makeText(contextForToast, "エラーが発生しました。\n(${e.message})", Toast.LENGTH_LONG).show()
            }
        }
        null
    } finally {
        if (isPremium) {
            VideoProcessingService.stop(context)
        }
    }
}

suspend fun saveVideoToGallery(context: Context, video: File) {
    withContext(Dispatchers.IO) {
        val resolver = context.contentResolver
        val values = ContentValues().apply {
            put(MediaStore.Video.Media.DISPLAY_NAME, "OshiCam_${System.currentTimeMillis()}.mp4")
            put(MediaStore.Video.Media.RELATIVE_PATH, "Movies/OshiCam")
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                put(MediaStore.Video.Media.IS_PENDING, 1)
            }
        }

        val uri = resolver.insert(MediaStore.Video.Media.EXTERNAL_CONTENT_URI, values)
        if (uri != null) {
            try {
                resolver.openOutputStream(uri)?.use { out ->
                    video.inputStream().use { it.copyTo(out) }
                }

                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    values.clear()
                    values.put(MediaStore.Video.Media.IS_PENDING, 0)
                    resolver.update(uri, values, null, null)
                }
            } catch (e: Exception) {
                resolver.delete(uri, null, null)
                Log.e("Gallery", "ギャラリー保存エラー", e)
            }
        }
    }
}

fun purchasePremium(activity: Activity, onResult: (Boolean) -> Unit) {
    try {
        Purchases.sharedInstance.getOfferingsWith(
            onSuccess = { offerings ->
                val packageToBuy = offerings.current?.monthly
                if (packageToBuy != null) {
                    val params = PurchaseParams.Builder(activity, packageToBuy).build()
                    Purchases.sharedInstance.purchaseWith(
                        params,
                        onSuccess = { _, customerInfo ->
                            if (customerInfo.entitlements["premium"]?.isActive == true) {
                                Toast.makeText(activity, "プレミアム版にアップグレードしました！", Toast.LENGTH_LONG).show()
                                onResult(true)
                            }
                        },
                        onError = { error, userCancelled ->
                            if (!userCancelled) {
                                Toast.makeText(activity, "エラー: ${error.message}", Toast.LENGTH_SHORT).show()
                            }
                        }
                    )
                } else {
                    Toast.makeText(activity, "プランが見つかりません", Toast.LENGTH_SHORT).show()
                }
            },
            onError = {
                Toast.makeText(activity, "通信エラーが発生しました", Toast.LENGTH_SHORT).show()
            }
        )
    } catch (e: Exception) {
        Toast.makeText(activity, "課金システムエラー: ${e.message}", Toast.LENGTH_LONG).show()
        e.printStackTrace()
    }
}
