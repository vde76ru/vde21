<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Logger;
use App\Core\Cache;
use OpenSearch\ClientBuilder;

class SearchService
{
    private static ?\OpenSearch\Client $client = null;
    
    public static function search(array $params): array
    {
        $requestId = uniqid('search_', true);
        $startTime = microtime(true);
        
        Logger::info("🔍 [$requestId] Search started", ['params' => $params]);
        
        try {
            $params = self::validateParams($params);
            
            // Проверяем ресурсы системы
            if (!self::checkSystemResources()) {
                throw new \Exception("System overloaded");
            }
            
            // Проверяем OpenSearch с новой логикой
            if (self::isOpenSearchAvailable()) {
                Logger::debug("✅ [$requestId] Using OpenSearch");
                $result = self::performOpenSearchWithTimeout($params, $requestId);
                
                $duration = round((microtime(true) - $startTime) * 1000, 2);
                Logger::info("✅ [$requestId] Completed in {$duration}ms");
                
                return $result;
            } else {
                Logger::warning("⚠️ [$requestId] OpenSearch unavailable, using MySQL");
                return self::searchViaMySQL($params);
            }
            
        } catch (\Exception $e) {
            $duration = round((microtime(true) - $startTime) * 1000, 2);
            Logger::error("❌ [$requestId] Failed after {$duration}ms", [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            // Правильный HTTP статус и структурированный ответ
            http_response_code(503);
            return [
                'success' => false,
                'error' => 'Search service temporarily unavailable',
                'error_code' => 'SERVICE_UNAVAILABLE',
                'products' => [],
                'total' => 0,
                'page' => $params['page'] ?? 1,
                'limit' => $params['limit'] ?? 20,
                'debug_info' => [
                    'request_id' => $requestId,
                    'duration_ms' => $duration,
                    'timestamp' => date('c')
                ]
            ];
        }
    }
    
    /**
     * Построение основного запроса с учетом всех условий
     */
    private static function buildMainQuery(string $query, array $words, bool $isArticul): array
    {
        $should = [];
        
        // 1. Точное совпадение артикула/SKU (максимальный приоритет)
        if ($isArticul) {
            $should[] = ['term' => ['external_id.keyword' => ['value' => $query, 'boost' => 1000]]];
            $should[] = ['term' => ['sku.keyword' => ['value' => $query, 'boost' => 900]]];
        }
        
        // 2. Префиксный поиск по артикулу
        $should[] = ['prefix' => ['external_id' => ['value' => $query, 'boost' => 100]]];
        $should[] = ['prefix' => ['sku' => ['value' => $query, 'boost' => 90]]];
        
        // 3. Поиск по артикулу с учетом ошибок (fuzzy)
        $should[] = [
            'fuzzy' => [
                'external_id' => [
                    'value' => $query,
                    'fuzziness' => 'AUTO',
                    'prefix_length' => 2,
                    'boost' => 80
                ]
            ]
        ];
        
        // 4. Точная фраза в названии
        $should[] = ['match_phrase' => ['name' => ['query' => $query, 'boost' => 70]]];
        
        // 5. Все слова должны присутствовать (AND)
        $should[] = [
            'match' => [
                'name' => [
                    'query' => $query,
                    'operator' => 'and',
                    'boost' => 60
                ]
            ]
        ];
        
        // 6. Поиск с учетом опечаток в названии
        $should[] = [
            'match' => [
                'name' => [
                    'query' => $query,
                    'fuzziness' => 'AUTO',
                    'prefix_length' => 3,
                    'boost' => 40
                ]
            ]
        ];
        
        // 7. Multi-match по всем важным полям
        $should[] = [
            'multi_match' => [
                'query' => $query,
                'fields' => [
                    'name^5',
                    'name.ngram^2',
                    'brand_name^3',
                    'series_name^2',
                    'description'
                ],
                'type' => 'best_fields',
                'fuzziness' => 'AUTO',
                'prefix_length' => 2,
                'boost' => 30
            ]
        ];
        
        // 8. Поиск по отдельным словам (для разбросанных слов)
        if (count($words) > 1) {
            $wordQueries = [];
            foreach ($words as $word) {
                if (mb_strlen($word) >= 2) {
                    $wordQueries[] = [
                        'multi_match' => [
                            'query' => $word,
                            'fields' => ['name^3', 'brand_name^2', 'description'],
                            'fuzziness' => 'AUTO',
                            'prefix_length' => 1
                        ]
                    ];
                }
            }
            
            $should[] = [
                'bool' => [
                    'should' => $wordQueries,
                    'minimum_should_match' => ceil(count($words) * 0.7),
                    'boost' => 20
                ]
            ];
        }
        
        // 9. N-gram поиск для частичных совпадений
        $should[] = [
            'match' => [
                'name.ngram' => [
                    'query' => $query,
                    'boost' => 10
                ]
            ]
        ];
        
        // 10. Wildcard поиск (последний вариант)
        if (mb_strlen($query) >= 3 && !$isArticul) {
            $should[] = [
                'wildcard' => [
                    'name.keyword' => [
                        'value' => "*{$query}*",
                        'boost' => 5
                    ]
                ]
            ];
        }
        
        return [
            'bool' => [
                'should' => $should,
                'minimum_should_match' => 1
            ]
        ];
    }
    
    /**
     * Функции скоринга для точной настройки релевантности
     */
    private static function buildScoringFunctions(string $query, bool $isArticul): array
    {
        $functions = [];
        
        // Бонус за популярность
        $functions[] = [
            'field_value_factor' => [
                'field' => 'popularity_score',
                'factor' => 1.2,
                'modifier' => 'log1p',
                'missing' => 0
            ],
            'weight' => 10
        ];
        
        // Бонус за наличие на складе
        $functions[] = [
            'filter' => ['term' => ['in_stock' => true]],
            'weight' => 5
        ];
        
        // Бонус за короткое название (обычно более релевантно)
        $functions[] = [
            'script_score' => [
                'script' => [
                    'source' => "Math.max(1, 50 - doc['name.keyword'].value.length()) / 50"
                ]
            ],
            'weight' => 3
        ];
        
        // Штраф за слишком длинное описание
        $functions[] = [
            'script_score' => [
                'script' => [
                    'source' => "
                        if (doc.containsKey('description') && doc['description'].size() > 0) {
                            return Math.max(0.5, 1 - (doc['description'].value.length() / 1000.0));
                        }
                        return 1;
                    "
                ]
            ],
            'weight' => 2
        ];
        
        return $functions;
    }
    
    /**
     * Автодополнение с исправлением ошибок
     */
    public static function autocomplete(string $query, int $limit = 10): array
    {
        $query = mb_strtolower(trim($query));
        if (mb_strlen($query) < 1) return [];
        
        try {
            if (!self::isOpenSearchAvailable()) {
                return self::autocompleteMysql($query, $limit);
            }
            
            // Используем suggest API для автодополнения
            $body = [
                'suggest' => [
                    'product-suggest' => [
                        'prefix' => $query,
                        'completion' => [
                            'field' => 'suggest',
                            'size' => $limit,
                            'fuzzy' => [
                                'fuzziness' => 'AUTO',
                                'prefix_length' => 1
                            ],
                            'contexts' => []
                        ]
                    ]
                ],
                // Дополнительно ищем по обычным полям
                'size' => $limit,
                '_source' => ['name', 'external_id', 'brand_name'],
                'query' => [
                    'bool' => [
                        'should' => [
                            ['prefix' => ['external_id' => ['value' => $query, 'boost' => 10]]],
                            ['prefix' => ['name.autocomplete' => ['value' => $query, 'boost' => 5]]],
                            ['match_phrase_prefix' => ['name' => ['query' => $query, 'boost' => 3]]],
                            ['fuzzy' => ['name' => ['value' => $query, 'fuzziness' => 'AUTO', 'boost' => 2]]],
                            ['prefix' => ['brand_name.autocomplete' => ['value' => $query, 'boost' => 2]]]
                        ]
                    ]
                ]
            ];
            
            $response = self::getClient()->search([
                'index' => 'products_current',
                'body' => $body
            ]);
            
            $suggestions = [];
            $seen = [];
            
            // Обрабатываем suggest результаты
            if (isset($response['suggest']['product-suggest'][0]['options'])) {
                foreach ($response['suggest']['product-suggest'][0]['options'] as $option) {
                    $text = $option['text'];
                    $key = mb_strtolower($text);
                    
                    if (!isset($seen[$key])) {
                        $suggestions[] = [
                            'text' => $text,
                            'type' => 'suggest',
                            'score' => $option['_score'] ?? 1
                        ];
                        $seen[$key] = true;
                    }
                }
            }
            
            // Добавляем результаты из обычного поиска
            foreach ($response['hits']['hits'] ?? [] as $hit) {
                $source = $hit['_source'];
                $text = $source['name'];
                $key = mb_strtolower($text);
                
                if (!isset($seen[$key])) {
                    $suggestions[] = [
                        'text' => $text,
                        'type' => 'product',
                        'score' => $hit['_score'],
                        'external_id' => $source['external_id'] ?? null
                    ];
                    $seen[$key] = true;
                    
                    if (count($suggestions) >= $limit) break;
                }
            }
            
            // Сортируем по score
            usort($suggestions, function($a, $b) {
                return ($b['score'] ?? 0) <=> ($a['score'] ?? 0);
            });
            
            return array_slice($suggestions, 0, $limit);
            
        } catch (\Exception $e) {
            Logger::warning('Autocomplete error', ['error' => $e->getMessage()]);
            return self::autocompleteMysql($query, $limit);
        }
    }
    
    /**
     * Определение является ли запрос артикулом
     */
    private static function isArticul(string $query): bool
    {
        // Артикул обычно содержит цифры и может иметь дефисы, точки
        return preg_match('/^[A-Za-z0-9\-\.\/]+$/', $query) && 
               preg_match('/\d/', $query) && 
               mb_strlen($query) <= 50;
    }
    
    private static function buildSort(string $sort, bool $hasQuery): array
    {
        switch ($sort) {
            case 'name':
                return [['name.keyword' => 'asc']];
            case 'external_id':
                return [['external_id.keyword' => 'asc']];
            case 'price_asc':
                return [['product_id' => 'asc']]; // Заменить на реальное поле цены
            case 'price_desc':
                return [['product_id' => 'desc']];
            case 'availability':
                return [['in_stock' => 'desc'], ['_score' => 'desc']];
            case 'popularity':
                return [['popularity_score' => 'desc'], ['_score' => 'desc']];
            case 'relevance':
            default:
                if ($hasQuery) {
                    return [['_score' => 'desc'], ['popularity_score' => 'desc']];
                } else {
                    return [['popularity_score' => 'desc'], ['name.keyword' => 'asc']];
                }
        }
    }
    
    private static function processResponse(array $response, array $params): array
    {
        $products = [];
        
        foreach ($response['hits']['hits'] ?? [] as $hit) {
            $product = $hit['_source'];
            $product['_score'] = $hit['_score'] ?? 0;
            
            if (isset($hit['highlight'])) {
                $product['_highlight'] = $hit['highlight'];
            }
            
            $products[] = $product;
        }
        
        // Обогащаем динамическими данными
        if (!empty($products)) {
            $productIds = array_column($products, 'product_id');
            $cityId = $params['city_id'] ?? 1;
            $userId = $params['user_id'] ?? null;
            
            try {
                $dynamicService = new DynamicProductDataService();
                $dynamicData = $dynamicService->getProductsDynamicData($productIds, $cityId, $userId);
                
                foreach ($products as &$product) {
                    $pid = $product['product_id'];
                    if (isset($dynamicData[$pid])) {
                        $product = array_merge($product, $dynamicData[$pid]);
                    }
                }
            } catch (\Exception $e) {
                Logger::warning('Failed to enrich products', ['error' => $e->getMessage()]);
            }
        }
        
        return [
            'products' => $products,
            'total' => $response['hits']['total']['value'] ?? 0,
            'page' => $params['page'],
            'limit' => $params['limit'],
            'max_score' => $response['hits']['max_score'] ?? 0
        ];
    }
    
    /**
     * MySQL fallback с улучшенной логикой
     */
    private static function searchViaMySQL(array $params): array
    {
        $query = $params['q'] ?? '';
        $page = $params['page'];
        $limit = $params['limit'];
        $offset = ($page - 1) * $limit;
        
        try {
            $pdo = Database::getConnection();
            
            $sql = "SELECT SQL_CALC_FOUND_ROWS 
                    p.product_id, p.external_id, p.sku, p.name, p.description,
                    p.brand_id, p.series_id, p.unit, p.min_sale, p.weight, p.dimensions,
                    b.name as brand_name, s.name as series_name,
                    CASE 
                        WHEN p.external_id = :exact_q THEN 1000
                        WHEN p.sku = :exact_q THEN 900
                        WHEN p.external_id LIKE :prefix_q THEN 100
                        WHEN p.sku LIKE :prefix_q THEN 90
                        WHEN p.name = :exact_q THEN 80
                        WHEN p.name LIKE :prefix_q THEN 50
                        WHEN p.name LIKE :search_q THEN 30
                        WHEN p.description LIKE :search_q THEN 10
                        WHEN b.name LIKE :search_q THEN 20
                        ELSE 1
                    END as relevance_score
                    FROM products p
                    LEFT JOIN brands b ON p.brand_id = b.brand_id
                    LEFT JOIN series s ON p.series_id = s.series_id
                    WHERE 1=1";
            
            $bindParams = [];
            
            if (!empty($query)) {
                $sql .= " AND (
                    p.external_id = :exact_q OR
                    p.sku = :exact_q OR
                    p.external_id LIKE :prefix_q OR
                    p.sku LIKE :prefix_q OR
                    p.name LIKE :search_q OR
                    p.description LIKE :search_q OR
                    b.name LIKE :search_q
                )";
                
                $bindParams['exact_q'] = $query;
                $bindParams['prefix_q'] = $query . '%';
                $bindParams['search_q'] = '%' . $query . '%';
            }
            
            // Сортировка
            switch ($params['sort']) {
                case 'name':
                    $sql .= " ORDER BY p.name ASC";
                    break;
                case 'external_id':
                    $sql .= " ORDER BY p.external_id ASC";
                    break;
                default:
                    if (!empty($query)) {
                        $sql .= " ORDER BY relevance_score DESC, p.name ASC";
                    } else {
                        $sql .= " ORDER BY p.name ASC";
                    }
                    break;
            }
            
            $sql .= " LIMIT :limit OFFSET :offset";
            
            $stmt = $pdo->prepare($sql);
            foreach ($bindParams as $key => $value) {
                $stmt->bindValue($key, $value);
            }
            $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
            $stmt->bindValue(':offset', $offset, \PDO::PARAM_INT);
            $stmt->execute();
            
            $products = $stmt->fetchAll();
            $total = $pdo->query("SELECT FOUND_ROWS()")->fetchColumn();
            
            return [
                'products' => $products,
                'total' => (int)$total,
                'page' => $page,
                'limit' => $limit
            ];
            
        } catch (\Exception $e) {
            Logger::error('MySQL search failed', ['error' => $e->getMessage()]);
            return [
                'products' => [],
                'total' => 0,
                'page' => $page,
                'limit' => $limit
            ];
        }
    }
    
    private static function autocompleteMysql(string $query, int $limit): array
    {
        try {
            $pdo = Database::getConnection();
            
            // Используем SOUNDEX для поиска похожих по звучанию слов
            $stmt = $pdo->prepare("
                SELECT DISTINCT p.name, p.external_id,
                    CASE 
                        WHEN p.external_id LIKE :exact THEN 100
                        WHEN p.external_id LIKE :prefix THEN 90
                        WHEN p.name LIKE :prefix THEN 50
                        WHEN p.name LIKE :anywhere THEN 30
                        WHEN SOUNDEX(p.name) = SOUNDEX(:soundex) THEN 20
                        ELSE 10
                    END as score
                FROM products p
                LEFT JOIN brands b ON p.brand_id = b.brand_id
                WHERE p.external_id LIKE :prefix OR 
                      p.name LIKE :anywhere OR 
                      b.name LIKE :prefix OR
                      SOUNDEX(p.name) = SOUNDEX(:soundex)
                ORDER BY score DESC, p.name ASC
                LIMIT :limit
            ");
            
            $stmt->bindValue(':exact', $query);
            $stmt->bindValue(':prefix', $query . '%');
            $stmt->bindValue(':anywhere', '%' . $query . '%');
            $stmt->bindValue(':soundex', $query);
            $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
            $stmt->execute();
            
            $suggestions = [];
            while ($row = $stmt->fetch()) {
                $suggestions[] = [
                    'text' => $row['name'],
                    'type' => 'product',
                    'external_id' => $row['external_id'],
                    'score' => $row['score']
                ];
            }
            
            return $suggestions;
            
        } catch (\Exception $e) {
            Logger::error('MySQL autocomplete failed', ['error' => $e->getMessage()]);
            return [];
        }
    }
    
    private static function validateParams(array $params): array
    {
        return [
            'q' => trim($params['q'] ?? ''),
            'page' => max(1, (int)($params['page'] ?? 1)),
            'limit' => min(100, max(1, (int)($params['limit'] ?? 20))),
            'city_id' => (int)($params['city_id'] ?? 1),
            'sort' => $params['sort'] ?? 'relevance',
            'user_id' => $params['user_id'] ?? null
        ];
    }
    
    private static function isOpenSearchAvailable(): bool
    {
        static $isAvailable = null;
        static $lastCheck = 0;
        static $consecutiveFailures = 0;
        
        // Периодически перепроверяем статус
        $checkInterval = min(300, 30 + ($consecutiveFailures * 10)); // От 30 до 300 сек
        
        if ($isAvailable !== null && (time() - $lastCheck) < $checkInterval) {
            return $isAvailable;
        }
        
        try {
            $startTime = microtime(true);
            $client = self::getClient();
            
            // Быстрая проверка с timeout
            $health = $client->cluster()->health([
                'timeout' => '5s',
                'wait_for_status' => null
            ]);
            
            $responseTime = (microtime(true) - $startTime) * 1000;
            
            // Проверяем и статус, и время отклика
            $acceptableStatuses = ['green', 'yellow'];
            $clusterStatus = $health['status'] ?? 'red';
            
            $isAvailable = in_array($clusterStatus, $acceptableStatuses) && $responseTime < 5000;
            
            if ($isAvailable) {
                $consecutiveFailures = 0;
            } else {
                $consecutiveFailures++;
                Logger::warning("⚠️ OpenSearch degraded", [
                    'status' => $clusterStatus,
                    'response_time_ms' => round($responseTime, 2),
                    'consecutive_failures' => $consecutiveFailures
                ]);
            }
            
            $lastCheck = time();
            
        } catch (\Exception $e) {
            $isAvailable = false;
            $lastCheck = time();
            $consecutiveFailures++;
            
            Logger::warning("❌ OpenSearch check failed", [
                'error' => $e->getMessage(),
                'consecutive_failures' => $consecutiveFailures
            ]);
        }
        
        return $isAvailable;
    }
    
    private static function getClient(): \OpenSearch\Client
    {
        if (self::$client === null) {
            self::$client = ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->setRetries(3)                    // Больше попыток
                ->setConnectionParams([
                    'timeout' => 20,               // HTTP timeout
                    'connect_timeout' => 5,        // Connection timeout
                    'client_timeout' => 25         // Общий timeout клиента
                ])
                ->setHandler(\OpenSearch\ClientBuilder::singleHandler()) // Одиночный обработчик
                ->build();
        }
        return self::$client;
    }
    
    private static function performOpenSearchWithTimeout(array $params, string $requestId): array
    {
        // Устанавливаем жесткие лимиты времени
        $originalTimeLimit = ini_get('max_execution_time');
        set_time_limit(30); // Максимум 30 секунд
        
        try {
            $body = [
                'timeout' => '15s', // Timeout на уровне OpenSearch
                'size' => $params['limit'],
                'from' => ($params['page'] - 1) * $params['limit'],
                'track_total_hits' => true,
                '_source' => [
                    'product_id', 'external_id', 'sku', 'name', 'description',
                    'brand_id', 'brand_name', 'series_id', 'series_name',
                    'unit', 'min_sale', 'weight', 'dimensions', 'images'
                ]
            ];
            
            // Ваша существующая логика построения запроса...
            if (!empty($params['q'])) {
                $query = mb_strtolower(trim($params['q']));
                $words = preg_split('/\s+/', $query, -1, PREG_SPLIT_NO_EMPTY);
                $isArticul = self::isArticul($query);

                $body['query'] = [
                    'function_score' => [
                        'query' => self::buildMainQuery($query, $words, $isArticul),
                        'functions' => self::buildScoringFunctions($query, $isArticul),
                        'score_mode' => 'sum',
                        'boost_mode' => 'multiply'
                    ]
                ];

                $body['highlight'] = [
                    'pre_tags' => ['<mark>'],
                    'post_tags' => ['</mark>'],
                    'fields' => [
                        'name' => ['number_of_fragments' => 0, 'fragment_size' => 300],
                        'external_id' => ['number_of_fragments' => 0],
                        'sku' => ['number_of_fragments' => 0],
                        'description' => ['fragment_size' => 150, 'number_of_fragments' => 1]
                    ]
                ];

                $body['rescore'] = [
                    'window_size' => 50,
                    'query' => [
                        'rescore_query' => [
                            'bool' => [
                                'should' => [
                                    ['match_phrase' => ['name' => ['query' => $query, 'boost' => 10]]],
                                    ['match' => ['name' => ['query' => $query, 'operator' => 'and', 'boost' => 5]]]
                                ]
                            ]
                        ],
                        'query_weight' => 0.7,
                        'rescore_query_weight' => 1.3
                    ]
                ];
            } else {
                $body['query'] = ['match_all' => new \stdClass()];
            }

            $body['sort'] = self::buildSort($params['sort'], !empty($params['q']));

            // Запрос с timeout'ами на всех уровнях
            $response = self::getClient()->search([
                'index' => 'products_current',
                'body' => $body,
                'client' => [
                    'timeout' => 20,          // HTTP timeout
                    'connect_timeout' => 5    // Connection timeout
                ]
            ]);

            return self::processResponse($response, $params);

        } catch (\Exception $e) {
            Logger::error("❌ [$requestId] OpenSearch timeout/error", [
                'error' => $e->getMessage(),
                'params' => $params
            ]);
            throw $e;
        } finally {
            // Восстанавливаем исходный лимит времени
            set_time_limit($originalTimeLimit);
        }
    }
    
    private static function checkSystemResources(): bool
    {
        // Проверка памяти
        $memoryUsage = memory_get_usage(true);
        $memoryLimit = self::parseMemoryLimit(ini_get('memory_limit'));
        
        if ($memoryUsage > $memoryLimit * 0.9) {
            Logger::warning("Memory usage critical", [
                'usage_mb' => round($memoryUsage / 1024 / 1024, 2),
                'limit_mb' => round($memoryLimit / 1024 / 1024, 2)
            ]);
            return false;
        }
        
        // Проверка нагрузки системы
        $load = sys_getloadavg();
        if ($load[0] > 10.0) {
            Logger::warning("System load high", ['load' => $load[0]]);
            return false;
        }
        
        return true;
    }

    private static function parseMemoryLimit(string $limit): int
    {
        $limit = trim($limit);
        $last = strtolower($limit[strlen($limit) - 1]);
        $limit = (int)$limit;
        
        switch ($last) {
            case 'g': $limit *= 1024 * 1024 * 1024; break;
            case 'm': $limit *= 1024 * 1024; break;
            case 'k': $limit *= 1024; break;
        }
        
        return $limit;
    }
}