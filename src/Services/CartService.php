<?php
namespace App\Services;

use App\Core\Database;
use App\Core\Logger;
use App\Exceptions\CartException;

/**
 * Единый сервис для работы с корзиной
 * Поддерживает как гостевые корзины (в сессии), так и пользовательские (в БД)
 */
class CartService
{
    const SESSION_KEY = 'cart';
    const MAX_ITEMS = 100;
    const MAX_QUANTITY = 9999;
    
    /**
     * Получить корзину для текущего пользователя/гостя
     */
    public static function get(?int $userId = null): array
    {
        if ($userId > 0) {
            return self::loadFromDatabase($userId);
        }
        
        return self::loadFromSession();
    }
    
    /**
     * Добавить товар в корзину
     */
    public static function add(int $productId, int $quantity = 1, ?int $userId = null): array
    {
        if ($productId <= 0 || $quantity <= 0) {
            throw new CartException('Некорректные данные товара');
        }
        
        if ($quantity > self::MAX_QUANTITY) {
            throw new CartException('Превышено максимальное количество товара');
        }
        
        $cart = self::get($userId);
        
        // Проверяем лимит товаров
        if (count($cart) >= self::MAX_ITEMS && !isset($cart[$productId])) {
            throw new CartException('Достигнут лимит товаров в корзине');
        }
        
        // Добавляем или обновляем товар
        if (isset($cart[$productId])) {
            $newQuantity = $cart[$productId]['quantity'] + $quantity;
            if ($newQuantity > self::MAX_QUANTITY) {
                throw new CartException('Превышено максимальное количество товара');
            }
            $cart[$productId]['quantity'] = $newQuantity;
        } else {
            $cart[$productId] = [
                'product_id' => $productId,
                'quantity' => $quantity,
                'added_at' => date('Y-m-d H:i:s')
            ];
        }
        
        self::save($cart, $userId);
        
        // Логируем добавление
        Logger::info('Товар добавлен в корзину', [
            'product_id' => $productId,
            'quantity' => $quantity,
            'user_id' => $userId
        ]);
        
        return $cart;
    }
    
    /**
     * Обновить количество товара
     */
    public static function update(int $productId, int $quantity, ?int $userId = null): array
    {
        if ($quantity <= 0) {
            return self::remove($productId, $userId);
        }
        
        if ($quantity > self::MAX_QUANTITY) {
            throw new CartException('Превышено максимальное количество товара');
        }
        
        $cart = self::get($userId);
        
        if (isset($cart[$productId])) {
            $cart[$productId]['quantity'] = $quantity;
            $cart[$productId]['updated_at'] = date('Y-m-d H:i:s');
            self::save($cart, $userId);
        }
        
        return $cart;
    }
    
    /**
     * Удалить товар из корзины
     */
    public static function remove(int $productId, ?int $userId = null): array
    {
        $cart = self::get($userId);
        unset($cart[$productId]);
        self::save($cart, $userId);
        
        Logger::info('Товар удален из корзины', [
            'product_id' => $productId,
            'user_id' => $userId
        ]);
        
        return $cart;
    }
    
    /**
     * Очистить корзину
     */
    public static function clear(?int $userId = null): void
    {
        self::save([], $userId);
        
        Logger::info('Корзина очищена', ['user_id' => $userId]);
    }
    
    /**
     * Получить корзину с полной информацией о товарах
     * Оптимизированная версия без N+1 запросов
     */
    public static function getWithProducts(?int $userId = null): array
    {
        $cart = self::get($userId);
        if (empty($cart)) {
            return ['cart' => [], 'products' => []];
        }
        
        $productIds = array_keys($cart);
        
        // Используем Database::query() вместо прямого PDO
        $placeholders = implode(',', array_fill(0, count($productIds), '?'));
        $stmt = Database::query(
            "SELECT p.*, pr.price as base_price, b.name as brand_name, s.name as series_name
             FROM products p
             LEFT JOIN prices pr ON pr.product_id = p.product_id AND pr.is_base = 1
             LEFT JOIN brands b ON p.brand_id = b.brand_id
             LEFT JOIN series s ON p.series_id = s.series_id
             WHERE p.product_id IN ($placeholders)",
            $productIds
        );
        
        $products = [];
        while ($row = $stmt->fetch()) {
            $products[$row['product_id']] = $row;
        }
        
        return ['cart' => $cart, 'products' => $products];
    }
    
    /**
     * Слияние гостевой корзины с пользовательской после логина
     */
    public static function mergeGuestCartWithUser(int $userId): void
    {
        if ($userId <= 0) return;
        
        $guestCart = self::loadFromSession();
        if (empty($guestCart)) return;
        
        $userCart = self::loadFromDatabase($userId);
        
        // Объединяем корзины
        foreach ($guestCart as $productId => $item) {
            if (isset($userCart[$productId])) {
                $newQuantity = $userCart[$productId]['quantity'] + $item['quantity'];
                $userCart[$productId]['quantity'] = min($newQuantity, self::MAX_QUANTITY);
            } else {
                $userCart[$productId] = $item;
            }
        }
        
        // Сохраняем объединенную корзину
        self::saveToDatabase($userId, $userCart);
        
        // Очищаем гостевую корзину
        self::clearSession();
        
        Logger::info('Корзины объединены', [
            'user_id' => $userId,
            'guest_items' => count($guestCart),
            'merged_items' => count($userCart)
        ]);
    }
    
    /**
     * Загрузить корзину из БД
     */
    private static function loadFromDatabase(int $userId): array
    {
        try {
            $stmt = Database::query(
                "SELECT payload FROM carts WHERE user_id = ? LIMIT 1",
                [$userId]
            );
            
            $row = $stmt->fetch();
            if ($row && $row['payload']) {
                $cart = json_decode($row['payload'], true);
                if (json_last_error() === JSON_ERROR_NONE) {
                    return $cart;
                }
            }
        } catch (\Exception $e) {
            Logger::error('Ошибка загрузки корзины из БД', [
                'user_id' => $userId,
                'error' => $e->getMessage()
            ]);
        }
        
        return [];
    }
    
    /**
     * Загрузить корзину из сессии
     */
    private static function loadFromSession(): array
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            session_start();
        }
        
        return $_SESSION[self::SESSION_KEY] ?? [];
    }
    
    /**
     * Сохранить корзину
     */
    private static function save(array $cart, ?int $userId = null): void
    {
        if ($userId > 0) {
            self::saveToDatabase($userId, $cart);
        }
        
        self::saveToSession($cart);
    }
    
    /**
     * Сохранить в БД
     */
    private static function saveToDatabase(int $userId, array $cart): void
    {
        try {
            $payload = json_encode($cart, JSON_UNESCAPED_UNICODE);
            
            Database::query(
                "INSERT INTO carts (user_id, payload, created_at, updated_at)
                 VALUES (?, ?, NOW(), NOW())
                 ON DUPLICATE KEY UPDATE 
                 payload = VALUES(payload),
                 updated_at = NOW()",
                [$userId, $payload]
            );
        } catch (\Exception $e) {
            Logger::error('Ошибка сохранения корзины в БД', [
                'user_id' => $userId,
                'error' => $e->getMessage()
            ]);
            throw new CartException('Не удалось сохранить корзину');
        }
    }
    
    /**
     * Сохранить в сессию
     */
    private static function saveToSession(array $cart): void
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            session_start();
        }
        
        $_SESSION[self::SESSION_KEY] = $cart;
        session_write_close();
    }
    
    /**
     * Очистить сессию
     */
    private static function clearSession(): void
    {
        if (session_status() !== PHP_SESSION_ACTIVE) {
            session_start();
        }
        
        unset($_SESSION[self::SESSION_KEY]);
        session_write_close();
    }
}