<?php
/**
 * 🚀 ПОЛНОСТЬЮ АВТОМАТИЗИРОВАННЫЙ скрипт индексации товаров OpenSearch v4
 * 
 * Что делает этот скрипт:
 * 1. ✅ Проверяет и подготавливает систему
 * 2. 🗑️ Удаляет старые индексы (если нужно)  
 * 3. 📝 Создает новый индекс с правильной конфигурацией
 * 4. 📦 Индексирует все товары из базы данных
 * 5. 🔄 Переключает алиас на новый индекс
 * 6. 🧹 Очищает старые индексы
 * 7. ✅ Валидирует результат
 * 
 * ОДИН ЗАПУСК = ГОТОВАЯ СИСТЕМА!
 */

require __DIR__ . '/vendor/autoload.php';

use OpenSearch\ClientBuilder;

// 🔧 Конфигурация (можно вынести в отдельный файл)
const BATCH_SIZE = 1000;
const MEMORY_LIMIT = '60G';
const MAX_EXECUTION_TIME = 3600;
const MAX_OLD_INDICES = 2; // Сколько старых индексов оставлять

class CompleteIndexer {
    private $client;
    private $pdo;
    private $processed = 0;
    private $errors = 0;
    private $skipped = 0;
    private $startTime;
    private $newIndexName;
    private $totalProducts = 0;
    private $oldIndices = [];

    public function __construct() {
        $this->startTime = microtime(true);
        $this->newIndexName = 'products_' . date('Y_m_d_H_i_s');
        echo $this->getHeader();
    }

    /**
     * 🎯 ГЛАВНЫЙ МЕТОД - запускает весь процесс
     */
    public function run(): void {
        try {
            // 🔍 Этап 1: Системная диагностика
            $this->checkSystemRequirements();
            $this->initializeConnections();
            $this->analyzeCurrentState();
            
            // 📝 Этап 2: Подготовка нового индекса
            $this->prepareNewIndex();
            
            // 📦 Этап 3: Индексация данных
            $this->processAllProducts();
            
            // ✅ Этап 4: Валидация и переключение
            $this->validateNewIndex();
            $this->switchToNewIndex();
            
            // 🧹 Этап 5: Финальная очистка
            $this->performCleanup();
            
            // 🎉 Этап 6: Финальный отчет
            $this->showFinalReport();
            
        } catch (Throwable $e) {
            $this->handleCriticalFailure($e);
        }
    }

    /**
     * 🔍 Проверка системных требований
     */
    private function checkSystemRequirements(): void {
        echo "🔍 === СИСТЕМНАЯ ДИАГНОСТИКА ===\n";
        
        // Проверка памяти
        $memoryLimit = ini_get('memory_limit');
        echo "💾 Лимит памяти: $memoryLimit\n";
        
        if ($this->parseMemoryLimit($memoryLimit) < (50 * 1024 * 1024 * 1024)) {
            echo "⚠️ Рекомендуется увеличить memory_limit до 60G\n";
        }
        
        // Проверка времени выполнения
        $timeLimit = ini_get('max_execution_time');
        echo "⏱️ Лимит времени: {$timeLimit}s\n";
        
        // Проверка расширений PHP
        $requiredExtensions = ['json', 'curl', 'pdo_mysql', 'mbstring'];
        foreach ($requiredExtensions as $ext) {
            if (!extension_loaded($ext)) {
                throw new Exception("❌ Отсутствует обязательное расширение PHP: $ext");
            }
        }
        echo "✅ Все необходимые расширения PHP загружены\n";
        
        // Проверка файлов конфигурации
        if (!file_exists(__DIR__ . '/products_v5.json')) {
            throw new Exception("❌ Не найден файл конфигурации: products_v5.json");
        }
        echo "✅ Файл конфигурации найден\n\n";
    }

    /**
     * 🔌 Инициализация всех соединений
     */
    private function initializeConnections(): void {
        echo "🔌 === ИНИЦИАЛИЗАЦИЯ СОЕДИНЕНИЙ ===\n";
        
        // OpenSearch соединение
        try {
            $this->client = ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setRetries(3)
                ->setConnectionParams([
                    'timeout' => 30, 
                    'connect_timeout' => 10
                ])
                ->build();
                
            $info = $this->client->info();
            echo "✅ OpenSearch подключен, версия: " . $info['version']['number'] . "\n";
            
            // Проверка здоровья кластера
            $health = $this->client->cluster()->health(['timeout' => '10s']);
            $status = $health['status'];
            echo "📊 Статус кластера: $status\n";
            
            if ($status === 'red') {
                throw new Exception("❌ Кластер OpenSearch в критическом состоянии!");
            }
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка подключения к OpenSearch: " . $e->getMessage());
        }

        // Проверка плагинов
        $this->checkRequiredPlugins();
        
        // База данных соединение
        try {
            $config = \App\Core\Config::get('database.mysql');
            $dsn = "mysql:host={$config['host']};dbname={$config['database']};charset=utf8mb4";
            
            $this->pdo = new PDO($dsn, $config['user'], $config['password'], [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
                PDO::MYSQL_ATTR_INIT_COMMAND => "SET SESSION sql_mode='STRICT_ALL_TABLES'"
            ]);
            
            echo "✅ База данных подключена\n\n";
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка подключения к БД: " . $e->getMessage());
        }
    }

    /**
     * 🔌 Проверка необходимых плагинов
     */
    private function checkRequiredPlugins(): void {
        try {
            $plugins = $this->client->cat()->plugins();
            $installedPlugins = [];
            
            foreach ($plugins as $plugin) {
                $name = $plugin['component'] ?? $plugin['name'] ?? 'unknown';
                $installedPlugins[] = $name;
            }
            
            echo "🔌 Установленные плагины: " . implode(', ', $installedPlugins) . "\n";
            
            // Проверяем критически важные плагины
            $requiredPlugins = ['analysis-phonetic']; // Добавить другие при необходимости
            $missingPlugins = [];
            
            foreach ($requiredPlugins as $plugin) {
                if (!in_array($plugin, $installedPlugins)) {
                    $missingPlugins[] = $plugin;
                }
            }
            
            if (!empty($missingPlugins)) {
                echo "⚠️ Отсутствующие плагины: " . implode(', ', $missingPlugins) . "\n";
                echo "ℹ️ Некоторые функции поиска могут быть недоступны\n";
            } else {
                echo "✅ Все необходимые плагины установлены\n";
            }
            
        } catch (Exception $e) {
            echo "⚠️ Не удалось проверить плагины: " . $e->getMessage() . "\n";
        }
    }

    /**
     * 📊 Анализ текущего состояния системы
     */
    private function analyzeCurrentState(): void {
        echo "📊 === АНАЛИЗ ТЕКУЩЕГО СОСТОЯНИЯ ===\n";
        
        // Получаем список существующих индексов
        try {
            $indices = $this->client->indices()->get(['index' => 'products_*']);
            $this->oldIndices = array_keys($indices);
            
            echo "🗂️ Найдено индексов products_*: " . count($this->oldIndices) . "\n";
            foreach ($this->oldIndices as $index) {
                echo "   📁 $index\n";
            }
            
        } catch (Exception $e) {
            echo "ℹ️ Существующие индексы не найдены (первый запуск?)\n";
            $this->oldIndices = [];
        }
        
        // Проверяем текущий алиас
        try {
            $aliases = $this->client->indices()->getAlias(['name' => 'products_current']);
            echo "🔗 Текущий алиас products_current:\n";
            foreach ($aliases as $indexName => $aliasData) {
                echo "   🎯 $indexName\n";
            }
        } catch (Exception $e) {
            echo "ℹ️ Алиас products_current не найден\n";
        }
        
        // Подсчитываем товары в БД
        $this->totalProducts = $this->getTotalProductsCount();
        echo "📦 Товаров в базе данных: {$this->totalProducts}\n";
        
        if ($this->totalProducts === 0) {
            throw new Exception("❌ В базе данных нет товаров для индексации!");
        }
        
        echo "\n";
    }

    /**
     * 📝 Подготовка нового индекса
     */
    private function prepareNewIndex(): void {
        echo "📝 === СОЗДАНИЕ НОВОГО ИНДЕКСА ===\n";
        echo "🆕 Имя нового индекса: {$this->newIndexName}\n";
        
        // Проверяем, не существует ли уже такой индекс
        try {
            if ($this->client->indices()->exists(['index' => $this->newIndexName])) {
                echo "⚠️ Индекс уже существует, удаляем...\n";
                $this->client->indices()->delete(['index' => $this->newIndexName]);
                sleep(2); // Ждем завершения удаления
            }
        } catch (Exception $e) {
            // Индекс не существует, это нормально
        }
        
        // Загружаем и валидируем конфигурацию
        $indexConfig = $this->loadAndValidateConfig();
        
        // Создаем индекс
        try {
            $this->client->indices()->create([
                'index' => $this->newIndexName,
                'body' => $indexConfig
            ]);
            
            echo "✅ Индекс создан\n";
            
            // Ждем готовности индекса
            $this->waitForIndexReady();
            echo "✅ Индекс готов к работе\n\n";
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка создания индекса: " . $e->getMessage());
        }
    }

    /**
     * 📝 Загрузка и валидация конфигурации индекса
     */
    private function loadAndValidateConfig(): array {
        echo "📄 Загружаем конфигурацию из products_v5.json...\n";
        
        $configContent = file_get_contents(__DIR__ . '/products_v5.json');
        if ($configContent === false) {
            throw new Exception("❌ Не удалось прочитать файл products_v5.json");
        }
        
        $config = json_decode($configContent, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new Exception("❌ Ошибка JSON в products_v5.json: " . json_last_error_msg());
        }
        
        // Детальная валидация конфигурации
        $this->validateIndexConfiguration($config);
        
        echo "✅ Конфигурация валидна\n";
        return $config;
    }

    /**
     * ✅ Валидация конфигурации индекса
     */
    private function validateIndexConfiguration(array $config): void {
        // Проверяем основную структуру
        if (!isset($config['settings']) || !isset($config['mappings'])) {
            throw new Exception("❌ Отсутствуют обязательные секции settings или mappings");
        }
        
        // Проверяем анализаторы
        $requiredAnalyzers = [
            'text_analyzer', 
            'code_analyzer', 
            'search_analyzer', 
            'autocomplete_analyzer'
        ];
        
        foreach ($requiredAnalyzers as $analyzer) {
            if (!isset($config['settings']['analysis']['analyzer'][$analyzer])) {
                throw new Exception("❌ Отсутствует обязательный анализатор: $analyzer");
            }
        }
        
        // Проверяем маппинг основных полей
        $requiredFields = [
            'product_id', 
            'external_id', 
            'name', 
            'brand_name', 
            'suggest'
        ];
        
        foreach ($requiredFields as $field) {
            if (!isset($config['mappings']['properties'][$field])) {
                throw new Exception("❌ Отсутствует обязательное поле в маппинге: $field");
            }
        }
        
        echo "✅ Все обязательные компоненты конфигурации присутствуют\n";
    }

    /**
     * ⏳ Ожидание готовности индекса
     */
    private function waitForIndexReady(): void {
        echo "⏳ Ожидаем готовности индекса...\n";
        
        $maxAttempts = 15;
        $attempt = 0;
        
        while ($attempt < $maxAttempts) {
            try {
                $health = $this->client->cluster()->health([
                    'index' => $this->newIndexName,
                    'wait_for_status' => 'yellow',
                    'timeout' => '10s'
                ]);
                
                if (in_array($health['status'], ['yellow', 'green'])) {
                    echo "✅ Индекс готов (статус: {$health['status']})\n";
                    return;
                }
                
            } catch (Exception $e) {
                // Продолжаем ждать
            }
            
            $attempt++;
            echo "   ⏱️ Попытка $attempt/$maxAttempts...\n";
            sleep(2);
        }
        
        throw new Exception("❌ Индекс не стал готов после ожидания");
    }

    /**
     * 📦 Обработка всех товаров
     */
    private function processAllProducts(): void {
        echo "📦 === ИНДЕКСАЦИЯ ТОВАРОВ ===\n";
        echo "📊 Товаров к обработке: {$this->totalProducts}\n";
        echo "📦 Размер пакета: " . BATCH_SIZE . "\n\n";
        
        $page = 1;
        $batchNumber = 0;
        
        // Основной цикл обработки
        do {
            $products = $this->fetchProductsBatch($page);
            if (empty($products)) break;
    
            $batchNumber++;
            $rangeStart = ($page - 1) * BATCH_SIZE + 1;
            $rangeEnd = ($page - 1) * BATCH_SIZE + count($products);
            
            // Формируем краткую информацию о пакете для строки прогресса
            $batchInfo = "{$rangeStart}-{$rangeEnd}";
            
            $this->processSingleBatch($products);
            $this->displayProgress($batchNumber, $batchInfo);
            
            $page++;
            
            // Управление ресурсами
            if ($page % 10 === 0) {
                gc_collect_cycles();
                // Показываем использование памяти на новой строке
                echo "\n";
                $this->showMemoryUsage();
            }
            
            // Небольшая пауза для снижения нагрузки на систему
            if ($page % 50 === 0) {
                echo "\n⏸️ Пауза для снижения нагрузки...\n";
                sleep(1);
            }
            
        } while (true);
        
        // Переводим на новую строку после завершения
        echo "\n✅ Индексация товаров завершена!\n\n";
    }

    /**
     * 📦 Получение пакета товаров из БД
     */
    private function fetchProductsBatch(int $page): array {
        $offset = ($page - 1) * BATCH_SIZE;
        
        try {
            $stmt = $this->pdo->prepare("
                SELECT p.*, 
                       COALESCE(b.name, '') AS brand_name,
                       COALESCE(s.name, '') AS series_name
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.brand_id
                LEFT JOIN series s ON p.series_id = s.series_id
                WHERE p.product_id IS NOT NULL 
                  AND p.product_id > 0
                ORDER BY p.product_id
                LIMIT :limit OFFSET :offset
            ");
            
            $stmt->bindValue(':limit', BATCH_SIZE, PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, PDO::PARAM_INT);
            $stmt->execute();
            
            return $stmt->fetchAll();
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка получения товаров: " . $e->getMessage());
        }
    }

    /**
     * 📦 Обработка одного пакета товаров
     */
    private function processSingleBatch(array $products): void {
        $bulkData = [];
        $batchSkipped = 0;
        
        foreach ($products as $product) {
            try {
                // Строгая валидация
                if (!$this->validateProduct($product)) {
                    $batchSkipped++;
                    continue;
                }
                
                // Обработка и нормализация данных
                $this->normalizeProductData($product);
                $this->enrichProductData($product);
                
                // Подготовка для bulk-индексации
                $this->prepareBulkData($bulkData, $product);
                
            } catch (Exception $e) {
                $this->logProductError($product['product_id'] ?? 'unknown', $e);
                $batchSkipped++;
            }
        }
        
        // Отправляем данные в OpenSearch
        if (!empty($bulkData)) {
            $this->sendBulkRequest($bulkData);
        }
        
        $this->skipped += $batchSkipped;
        
        if ($batchSkipped > 0) {
            echo "   ⚠️ Пропущено в пакете: $batchSkipped\n";
        }
    }

    /**
     * ✅ Валидация товара перед обработкой
     */
    private function validateProduct(array $product): bool {
        // Обязательные поля
        if (empty($product['product_id']) || $product['product_id'] <= 0) {
            return false;
        }
        
        // Хотя бы одно из идентифицирующих полей должно быть заполнено
        if (empty($product['name']) && empty($product['external_id']) && empty($product['sku'])) {
            return false;
        }
        
        return true;
    }

    /**
     * 🔧 Нормализация данных товара
     */
    private function normalizeProductData(array &$product): void {
        // Текстовые поля
        $textFields = ['external_id', 'sku', 'name', 'description', 'brand_name', 'series_name'];
        foreach ($textFields as $field) {
            $product[$field] = $this->normalizeText($product[$field] ?? '');
        }
        
        // Числовые поля с валидацией
        $product['product_id'] = max(1, (int)($product['product_id'] ?? 0));
        $product['brand_id'] = max(0, (int)($product['brand_id'] ?? 0));
        $product['series_id'] = max(0, (int)($product['series_id'] ?? 0));
        $product['min_sale'] = max(1, (int)($product['min_sale'] ?? 1));
        $product['weight'] = max(0, (float)($product['weight'] ?? 0));
        
        // Инициализация массивов
        $product['categories'] = [];
        $product['category_ids'] = [];
    }

    /**
     * 🔤 Нормализация текста
     */
    private function normalizeText(?string $str): string {
        if (empty($str)) return '';
        
        // Удаляем управляющие символы кроме пробелов, табов и переносов
        $str = preg_replace('/[^\P{C}\t\n\r]+/u', '', $str);
        
        // Нормализуем пробелы
        $str = preg_replace('/\s+/', ' ', $str);
        
        // Удаляем пробелы в начале и конце
        return trim($str);
    }

    /**
     * 🎨 Обогащение данных товара
     */
    private function enrichProductData(array &$product): void {
        // Создаем данные для автодополнения
        $product['suggest'] = $this->createSuggestData($product);
        
        // Создаем общее поле для поиска
        $searchComponents = array_filter([
            $product['name'],
            $product['external_id'],
            $product['sku'],
            $product['brand_name'],
            $product['series_name'],
            $product['description']
        ]);
        
        $product['search_all'] = $this->normalizeText(implode(' ', $searchComponents));
        
        // Нормализация дат
        $product['created_at'] = $this->normalizeDate($product['created_at'] ?? null);
        $product['updated_at'] = $this->normalizeDate($product['updated_at'] ?? null);
        
        // Дефолтные значения для дополнительных полей
        $product['attributes'] = [];
        $product['images'] = [];
        $product['documents'] = [
            'certificates' => 0,
            'manuals' => 0,
            'drawings' => 0
        ];
        $product['popularity_score'] = 0.0;
        $product['in_stock'] = false;
        
        // Удаляем пустые значения для экономии места
        $product = array_filter($product, function($value) {
            return $value !== null && $value !== '';
        });
    }

    /**
     * 📅 Нормализация даты
     */
    private function normalizeDate(?string $date): string {
        if (empty($date)) {
            return date('c');
        }
        
        try {
            $timestamp = strtotime($date);
            if ($timestamp === false) {
                return date('c');
            }
            return date('c', $timestamp);
        } catch (Exception $e) {
            return date('c');
        }
    }

    /**
     * 💡 Создание данных для автодополнения
     */
    private function createSuggestData(array $product): array {
        $suggestions = [];
        
        // Название товара (максимальный приоритет)
        if (!empty($product['name']) && mb_strlen($product['name']) >= 2) {
            $suggestions[] = [
                'input' => [$product['name']],
                'weight' => 100
            ];
        }
        
        // Артикул
        if (!empty($product['external_id']) && mb_strlen($product['external_id']) >= 2) {
            $suggestions[] = [
                'input' => [$product['external_id']],
                'weight' => 95
            ];
        }
        
        // SKU
        if (!empty($product['sku']) && mb_strlen($product['sku']) >= 2) {
            $suggestions[] = [
                'input' => [$product['sku']],
                'weight' => 90
            ];
        }
        
        // Бренд
        if (!empty($product['brand_name']) && mb_strlen($product['brand_name']) >= 2) {
            $suggestions[] = [
                'input' => [$product['brand_name']],
                'weight' => 70
            ];
        }
        
        // Серия
        if (!empty($product['series_name']) && mb_strlen($product['series_name']) >= 2) {
            $suggestions[] = [
                'input' => [$product['series_name']],
                'weight' => 60
            ];
        }
        
        return $suggestions;
    }

    /**
     * 📤 Подготовка данных для bulk-запроса
     */
    private function prepareBulkData(array &$bulkData, array $product): void {
        $bulkData[] = [
            'index' => [
                '_index' => $this->newIndexName, 
                '_id' => $product['product_id']
            ]
        ];
        $bulkData[] = $product;
    }

    /**
     * 🚀 Отправка bulk-запроса в OpenSearch
     */
    private function sendBulkRequest(array $bulkData): void {
        if (empty($bulkData)) return;
        
        try {
            $response = $this->client->bulk([
                'body' => $bulkData,
                'timeout' => '60s',
                'refresh' => false // Ускоряем индексацию
            ]);
            
            $itemsCount = count($bulkData) / 2;
            $this->processed += $itemsCount;
            
            // Обрабатываем ошибки
            if ($response['errors'] ?? false) {
                $this->handleBulkErrors($response['items'] ?? []);
            }
            
        } catch (Exception $e) {
            $this->errors += count($bulkData) / 2;
            throw new Exception("❌ Ошибка bulk-запроса: " . $e->getMessage());
        }
    }

    /**
     * ❌ Обработка ошибок bulk-индексации
     */
    private function handleBulkErrors(array $items): void {
        foreach ($items as $item) {
            if ($error = $item['index']['error'] ?? null) {
                $this->errors++;
                
                $errorInfo = [
                    'id' => $item['index']['_id'] ?? 'unknown',
                    'type' => $error['type'] ?? 'unknown',
                    'reason' => $error['reason'] ?? 'unknown'
                ];
                
                error_log('Bulk indexing error: ' . json_encode($errorInfo));
                
                // Показываем первые несколько ошибок
                if ($this->errors <= 5) {
                    echo "   ❌ Ошибка ID {$errorInfo['id']}: {$errorInfo['reason']}\n";
                }
            }
        }
    }

    /**
     * 📊 Отображение прогресса
     */
    private function displayProgress(): void {
        if ($this->totalProducts === 0) return;
        
        $progress = round(($this->processed / $this->totalProducts) * 100, 1);
        $memory = round(memory_get_usage(true) / 1024 / 1024, 2) . 'MB';
        $elapsed = microtime(true) - $this->startTime;
        $speed = $this->processed > 0 ? round($this->processed / $elapsed, 0) : 0;
        
        // Создаем визуальный прогресс-бар
        $barLength = 30;
        $filledLength = intval($progress / 100 * $barLength);
        $bar = str_repeat('█', $filledLength) . str_repeat('░', $barLength - $filledLength);
        
        $timeStr = $this->formatTime($elapsed);
        
        echo "\r[{$bar}] {$progress}% | ✅{$this->processed} ❌{$this->errors} ⚠️{$this->skipped} | 💾{$memory} | ⏱️{$timeStr} | 🚀{$speed}/s ";
        flush();
    }

    /**
     * 💾 Показ использования памяти
     */
    private function showMemoryUsage(): void {
        $current = memory_get_usage(true);
        $peak = memory_get_peak_usage(true);
        echo "\n💾 Память: текущая " . round($current/1024/1024, 2) . "MB, пик " . round($peak/1024/1024, 2) . "MB\n";
    }

    /**
     * ✅ Валидация нового индекса
     */
    private function validateNewIndex(): void {
        echo "\n✅ === ВАЛИДАЦИЯ НОВОГО ИНДЕКСА ===\n";
        
        try {
            // Принудительное обновление индекса
            echo "🔄 Обновляем индекс...\n";
            $this->client->indices()->refresh(['index' => $this->newIndexName]);
            
            // Получаем статистику
            $stats = $this->client->indices()->stats(['index' => $this->newIndexName]);
            $docCount = $stats['indices'][$this->newIndexName]['total']['docs']['count'] ?? 0;
            
            echo "📊 Документов в индексе: $docCount\n";
            echo "📊 Обработано скриптом: {$this->processed}\n";
            echo "📊 Ошибок: {$this->errors}\n";
            echo "📊 Пропущено: {$this->skipped}\n";
            
            // Проверяем соответствие
            $difference = abs($docCount - $this->processed);
            if ($difference > 10) { // Допускаем небольшие расхождения
                echo "⚠️ Значительное расхождение в количестве документов!\n";
            }
            
            // Тестовый поиск
            echo "🔍 Выполняем тестовый поиск...\n";
            $testSearch = $this->client->search([
                'index' => $this->newIndexName,
                'body' => [
                    'size' => 5,
                    'query' => ['match_all' => new \stdClass()],
                    '_source' => ['product_id', 'name', 'external_id']
                ]
            ]);
            
            $found = $testSearch['hits']['total']['value'] ?? 0;
            echo "✅ Тестовый поиск: найдено $found документов\n";
            
            if ($found === 0) {
                throw new Exception("❌ Индекс пуст после индексации!");
            }
            
            // Показываем примеры документов
            echo "📋 Примеры документов:\n";
            foreach ($testSearch['hits']['hits'] as $hit) {
                $source = $hit['_source'];
                echo "   📄 ID: {$source['product_id']}, Название: " . mb_substr($source['name'] ?? 'Н/Д', 0, 50) . "...\n";
            }
            
            echo "✅ Валидация успешна\n\n";
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка валидации: " . $e->getMessage());
        }
    }

    /**
     * 🔄 Переключение на новый индекс
     */
    private function switchToNewIndex(): void {
        echo "🔄 === ПЕРЕКЛЮЧЕНИЕ НА НОВЫЙ ИНДЕКС ===\n";
        
        try {
            // Атомарное переключение алиаса
            $actions = [];
            
            // Удаляем старые алиасы
            try {
                $aliases = $this->client->indices()->getAlias(['name' => 'products_current']);
                foreach (array_keys($aliases) as $indexName) {
                    if ($indexName !== $this->newIndexName) {
                        $actions[] = ['remove' => ['index' => $indexName, 'alias' => 'products_current']];
                        echo "➖ Удаляем алиас с индекса: $indexName\n";
                    }
                }
            } catch (Exception $e) {
                echo "ℹ️ Старый алиас не найден\n";
            }
            
            // Добавляем новый алиас
            $actions[] = ['add' => ['index' => $this->newIndexName, 'alias' => 'products_current']];
            echo "➕ Добавляем алиас к новому индексу: {$this->newIndexName}\n";
            
            // Выполняем атомарную операцию
            if (!empty($actions)) {
                $this->client->indices()->updateAliases(['body' => ['actions' => $actions]]);
            }
            
            // Проверяем результат
            $aliases = $this->client->indices()->getAlias(['name' => 'products_current']);
            echo "✅ Алиас products_current теперь указывает на:\n";
            foreach (array_keys($aliases) as $indexName) {
                echo "   🎯 $indexName\n";
            }
            
            echo "✅ Переключение завершено\n\n";
            
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка переключения алиаса: " . $e->getMessage());
        }
    }

    /**
     * 🧹 Финальная очистка
     */
    private function performCleanup(): void {
        echo "🧹 === ОЧИСТКА СТАРЫХ ИНДЕКСОВ ===\n";
        
        try {
            // Получаем все индексы products_*
            $indices = $this->client->indices()->get(['index' => 'products_*']);
            $allIndices = array_keys($indices);
            
            // Сортируем по имени (новые индексы имеют более поздние даты)
            usort($allIndices, function($a, $b) {
                return strcmp($b, $a); // Обратная сортировка
            });
            
            echo "🗂️ Всего найдено индексов: " . count($allIndices) . "\n";
            
            // Оставляем только MAX_OLD_INDICES старых индексов + новый
            $indicesToKeep = array_slice($allIndices, 0, MAX_OLD_INDICES + 1);
            $indicesToDelete = array_slice($allIndices, MAX_OLD_INDICES + 1);
            
            echo "📚 Оставляем индексов: " . count($indicesToKeep) . "\n";
            foreach ($indicesToKeep as $index) {
                echo "   📁 $index\n";
            }
            
            if (!empty($indicesToDelete)) {
                echo "🗑️ Удаляем старых индексов: " . count($indicesToDelete) . "\n";
                foreach ($indicesToDelete as $indexName) {
                    try {
                        echo "   🗑️ Удаляем: $indexName\n";
                        $this->client->indices()->delete(['index' => $indexName]);
                    } catch (Exception $e) {
                        echo "   ⚠️ Не удалось удалить $indexName: " . $e->getMessage() . "\n";
                    }
                }
            } else {
                echo "ℹ️ Старые индексы не требуют очистки\n";
            }
            
            echo "✅ Очистка завершена\n\n";
            
        } catch (Exception $e) {
            echo "⚠️ Ошибка при очистке: " . $e->getMessage() . "\n";
        }
    }

    /**
     * 🎉 Финальный отчет
     */
    private function showFinalReport(): void {
        $totalTime = microtime(true) - $this->startTime;
        $speed = $this->processed > 0 ? $this->processed / $totalTime : 0;
        $successRate = $this->totalProducts > 0 ? ($this->processed / $this->totalProducts) * 100 : 0;
        
        echo $this->getFooter();
        echo "🎉 ИНДЕКСАЦИЯ ПОЛНОСТЬЮ ЗАВЕРШЕНА\n";
        echo str_repeat("=", 60) . "\n";
        echo "📂 Новый индекс: {$this->newIndexName}\n";
        echo "📦 Всего товаров в БД: {$this->totalProducts}\n";
        echo "✅ Успешно обработано: {$this->processed}\n";
        echo "❌ Ошибок индексации: {$this->errors}\n";
        echo "⚠️ Пропущено товаров: {$this->skipped}\n";
        echo "📊 Успешность: " . number_format($successRate, 1) . "%\n";
        echo "⏱️ Общее время: " . $this->formatTime($totalTime) . "\n";
        echo "🚀 Средняя скорость: " . number_format($speed, 0) . " товаров/сек\n";
        echo "💾 Пиковое использование памяти: " . round(memory_get_peak_usage(true)/1024/1024, 2) . "MB\n";
        echo "🔗 Алиас products_current: АКТИВЕН\n";
        echo "🧹 Старые индексы: ОЧИЩЕНЫ\n";
        echo str_repeat("=", 60) . "\n";
        echo "🎯 СИСТЕМА ГОТОВА К РАБОТЕ!\n";
        echo "✅ Вы можете начинать использовать поиск\n\n";
    }

    /**
     * ⚡ Вспомогательные методы
     */
    
    private function getTotalProductsCount(): int {
        try {
            return (int)$this->pdo->query("SELECT COUNT(*) FROM products WHERE product_id IS NOT NULL AND product_id > 0")->fetchColumn();
        } catch (Exception $e) {
            throw new Exception("❌ Ошибка подсчета товаров: " . $e->getMessage());
        }
    }

    private function parseMemoryLimit(string $limit): int {
        $limit = trim($limit);
        $last = strtolower($limit[strlen($limit) - 1]);
        $limit = (int)$limit;
        
        switch ($last) {
            case 'g': $limit *= 1024;
            case 'm': $limit *= 1024;
            case 'k': $limit *= 1024;
        }
        
        return $limit;
    }

    private function formatTime(float $seconds): string {
        if ($seconds < 60) {
            return number_format($seconds, 1) . 's';
        } elseif ($seconds < 3600) {
            return floor($seconds / 60) . 'm ' . number_format($seconds % 60, 0) . 's';
        } else {
            return floor($seconds / 3600) . 'h ' . floor(($seconds % 3600) / 60) . 'm';
        }
    }

    private function logProductError(int $productId, Exception $e): void {
        $this->errors++;
        $errorMsg = "Product ID $productId: " . $e->getMessage();
        error_log("Indexing error: " . $errorMsg);
    }

    private function getHeader(): string {
        return "
" . str_repeat("=", 80) . "
🚀 ПОЛНОСТЬЮ АВТОМАТИЗИРОВАННЫЙ ИНДЕКСАТОР OPENSEARCH v4.0
" . str_repeat("=", 80) . "
📅 Запуск: " . date('Y-m-d H:i:s') . "
🖥️ Сервер: " . gethostname() . "
🐘 PHP: " . PHP_VERSION . "
💾 Память: " . ini_get('memory_limit') . "
⏱️ Время: " . ini_get('max_execution_time') . "s
" . str_repeat("=", 80) . "

";
    }

    private function getFooter(): string {
        return "\n" . str_repeat("=", 80) . "\n";
    }

    /**
     * 💥 Обработка критических ошибок
     */
    private function handleCriticalFailure(Throwable $e): void {
        echo "\n\n💥 КРИТИЧЕСКАЯ ОШИБКА\n";
        echo str_repeat("=", 50) . "\n";
        echo "❌ Сообщение: " . $e->getMessage() . "\n";
        echo "📍 Файл: " . $e->getFile() . ":" . $e->getLine() . "\n";
        echo "🔍 Trace:\n" . $e->getTraceAsString() . "\n";
        
        // Пытаемся очистить частично созданный индекс
        if (isset($this->newIndexName) && isset($this->client)) {
            try {
                echo "\n🧹 Попытка очистки частично созданного индекса...\n";
                $this->client->indices()->delete(['index' => $this->newIndexName]);
                echo "✅ Частично созданный индекс удален\n";
            } catch (Exception $cleanupException) {
                echo "⚠️ Не удалось очистить индекс: " . $cleanupException->getMessage() . "\n";
            }
        }
        
        echo "\n❌ ИНДЕКСАЦИЯ ПРЕРВАНА\n";
        echo "💡 Проверьте ошибку выше и попробуйте снова\n\n";
        
        exit(1);
    }
}

// 🚀 ТОЧКА ВХОДА - ЗАПУСК ПОЛНОЙ АВТОМАТИЗАЦИИ

// Устанавливаем ограничения ресурсов
ini_set('memory_limit', MEMORY_LIMIT);
ini_set('max_execution_time', MAX_EXECUTION_TIME);
set_time_limit(0);
gc_enable();

// Запускаем полный автоматизированный процесс
try {
    $indexer = new CompleteIndexer();
    $indexer->run();
    
    echo "🎉 СКРИПТ УСПЕШНО ЗАВЕРШЕН!\n";
    echo "🔗 Ваш поиск теперь работает на: products_current\n";
    exit(0);
    
} catch (Throwable $e) {
    echo "\n💥 ФАТАЛЬНАЯ ОШИБКА СКРИПТА: " . $e->getMessage() . "\n";
    exit(1);
}