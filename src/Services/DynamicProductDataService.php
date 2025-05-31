<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Cache;
use App\Core\Logger;
use PDO;

/**
 * Оптимизированный сервис для динамических данных товаров
 */
class DynamicProductDataService
{
    private const CACHE_TTL = 300;
    private const MAX_BATCH_SIZE = 1000;
    
    /**
     * Получить динамические данные с улучшенной обработкой ошибок
     */
    public function getProductsDynamicData(array $productIds, int $cityId, ?int $userId = null): array
    {
        try {
            // Валидация
            $productIds = array_values(array_unique(array_filter($productIds, 'is_numeric')));
            if (empty($productIds) || count($productIds) > self::MAX_BATCH_SIZE) {
                return [];
            }

            // Проверка города - КРИТИЧЕСКИ ВАЖНО!
            if (!$this->cityExists($cityId)) {
                Logger::error('City not found', ['city_id' => $cityId]);
                // Возвращаем данные с дефолтными значениями вместо ошибки
                return $this->getDefaultDataForProducts($productIds);
            }

            // Кеш
            $cacheKey = $this->getCacheKey($productIds, $cityId, $userId);
            $cached = Cache::get($cacheKey);
            if ($cached !== null) {
                return $cached;
            }

            // Собираем данные безопасно
            $result = $this->collectProductData($productIds, $cityId, $userId);
            
            Cache::set($cacheKey, $result, self::CACHE_TTL);
            return $result;
            
        } catch (\Exception $e) {
            Logger::error('DynamicProductDataService critical error', [
                'error' => $e->getMessage(),
                'city_id' => $cityId,
                'products_count' => count($productIds)
            ]);
            
            // Возвращаем безопасные дефолтные данные
            return $this->getDefaultDataForProducts($productIds);
        }
    }
    
    /**
     * Проверка существования города
     */
    private function cityExists(int $cityId): bool
    {
        try {
            $stmt = Database::query("SELECT 1 FROM cities WHERE city_id = ? LIMIT 1", [$cityId]);
            return (bool)$stmt->fetch();
        } catch (\Exception $e) {
            return false;
        }
    }
    
    /**
     * Безопасный сбор данных
     */
    private function collectProductData(array $productIds, int $cityId, ?int $userId): array
    {
        $pdo = Database::getConnection();
        $result = [];
        
        // 1. Базовая инициализация для всех товаров
        foreach ($productIds as $productId) {
            $result[$productId] = $this->getDefaultProductData();
        }
        
        // 2. Цены - простой запрос
        $prices = $this->getPricesSimple($pdo, $productIds, $userId);
        foreach ($prices as $productId => $price) {
            $result[$productId]['price'] = $price;
        }
        
        // 3. Остатки - упрощенный запрос
        $stocks = $this->getStocksSimple($pdo, $productIds, $cityId);
        foreach ($stocks as $productId => $stock) {
            $result[$productId]['stock'] = $stock;
            $result[$productId]['available'] = $stock['quantity'] > 0;
        }
        
        // 4. Доставка - базовая логика
        foreach ($productIds as $productId) {
            $hasStock = $result[$productId]['stock']['quantity'] > 0;
            $result[$productId]['delivery'] = $this->getDeliveryInfo($hasStock, $cityId);
        }
        
        return $result;
    }
    
    /**
     * Упрощенное получение цен
     */
    private function getPricesSimple(PDO $pdo, array $productIds, ?int $userId): array
    {
        $prices = [];
        $placeholders = implode(',', array_fill(0, count($productIds), '?'));
        
        try {
            // Только базовые цены
            $sql = "SELECT product_id, price 
                    FROM prices 
                    WHERE product_id IN ($placeholders) 
                      AND is_base = 1 
                      AND (valid_to IS NULL OR valid_to >= CURDATE())
                    GROUP BY product_id";
            
            $stmt = $pdo->prepare($sql);
            $stmt->execute($productIds);
            
            while ($row = $stmt->fetch()) {
                $price = (float)$row['price'];
                $prices[$row['product_id']] = [
                    'base' => $price,
                    'final' => $price,
                    'has_special' => false
                ];
            }
        } catch (\Exception $e) {
            Logger::warning('Price fetch failed', ['error' => $e->getMessage()]);
        }
        
        return $prices;
    }
    
    /**
     * Упрощенное получение остатков
     */
    private function getStocksSimple(PDO $pdo, array $productIds, int $cityId): array
    {
        $stocks = [];
        $placeholders = implode(',', array_fill(0, count($productIds), '?'));
        
        try {
            // Получаем склады города
            $warehouseSql = "SELECT warehouse_id FROM city_warehouse_mapping WHERE city_id = ?";
            $stmt = $pdo->prepare($warehouseSql);
            $stmt->execute([$cityId]);
            $warehouseIds = $stmt->fetchAll(PDO::FETCH_COLUMN);
            
            if (empty($warehouseIds)) {
                Logger::info('No warehouses for city', ['city_id' => $cityId]);
                return $stocks;
            }
            
            // Получаем остатки
            $whPlaceholders = implode(',', array_fill(0, count($warehouseIds), '?'));
            $stockSql = "SELECT product_id, SUM(quantity - reserved) as available
                        FROM stock_balances
                        WHERE product_id IN ($placeholders)
                          AND warehouse_id IN ($whPlaceholders)
                          AND quantity > reserved
                        GROUP BY product_id";
            
            $stmt = $pdo->prepare($stockSql);
            $stmt->execute(array_merge($productIds, $warehouseIds));
            
            while ($row = $stmt->fetch()) {
                $stocks[$row['product_id']] = [
                    'quantity' => (int)$row['available'],
                    'warehouses' => []
                ];
            }
        } catch (\Exception $e) {
            Logger::warning('Stock fetch failed', ['error' => $e->getMessage()]);
        }
        
        return $stocks;
    }
    
    /**
     * Базовая информация о доставке
     */
    private function getDeliveryInfo(bool $hasStock, int $cityId): array
    {
        if ($hasStock) {
            // В наличии - доставка завтра
            $date = new \DateTime('tomorrow');
            return [
                'date' => $date->format('d.m.Y'),
                'text' => 'Завтра'
            ];
        } else {
            // Под заказ - через 3 дня
            $date = new \DateTime('+3 days');
            return [
                'date' => $date->format('d.m.Y'),
                'text' => 'Под заказ'
            ];
        }
    }
    
    /**
     * Дефолтные данные для товара
     */
    private function getDefaultProductData(): array
    {
        return [
            'price' => [
                'base' => null,
                'final' => null,
                'has_special' => false
            ],
            'stock' => [
                'quantity' => 0,
                'warehouses' => []
            ],
            'delivery' => [
                'date' => null,
                'text' => 'Уточняйте'
            ],
            'available' => false
        ];
    }
    
    /**
     * Дефолтные данные для списка товаров
     */
    private function getDefaultDataForProducts(array $productIds): array
    {
        $result = [];
        foreach ($productIds as $productId) {
            $result[$productId] = $this->getDefaultProductData();
        }
        return $result;
    }
    
    /**
     * Ключ кеша
     */
    private function getCacheKey(array $productIds, int $cityId, ?int $userId): string
    {
        sort($productIds);
        return 'dynamic:' . md5(implode(',', $productIds) . ':' . $cityId . ':' . ($userId ?? 0));
    }
}