<?php
namespace App\Controllers;

use App\Services\SearchService;
use App\Services\DynamicProductDataService;
use App\DTO\ProductAvailabilityDTO;
use App\Services\AuthService;
use App\Core\Logger;

class ApiController extends BaseController
{
    /**
     * GET /api/availability - Получение данных о наличии товаров
     */
    public function availabilityAction(): void
    {
        try {
            // Простая валидация параметров
            $cityId = (int)($_GET['city_id'] ?? 1);
            $productIdsStr = trim($_GET['product_ids'] ?? '');
            
            if ($cityId < 1) {
                $this->error('Неверный city_id', 400);
                return;
            }
            
            if (empty($productIdsStr)) {
                $this->error('Параметр product_ids обязателен', 400);
                return;
            }
            
            // Парсим и валидируем product_ids
            $productIds = array_map('intval', explode(',', $productIdsStr));
            $productIds = array_filter($productIds, fn($id) => $id > 0);
            $productIds = array_unique($productIds);
            
            if (empty($productIds)) {
                $this->error('Нет валидных product_ids', 400);
                return;
            }
            
            if (count($productIds) > 1000) {
                $this->error('Слишком много товаров, максимум 1000', 400);
                return;
            }
            
            // Получаем динамические данные
            $dynamicService = new DynamicProductDataService();
            $userId = AuthService::check() ? AuthService::user()['id'] : null;
            
            $dynamicData = $dynamicService->getProductsDynamicData($productIds, $cityId, $userId);
            
            // Преобразуем в DTO формат
            $result = [];
            foreach ($productIds as $productId) {
                $data = $dynamicData[$productId] ?? [];
                $dto = ProductAvailabilityDTO::fromDynamicData($productId, $data);
                $result[$productId] = $dto->toArray();
            }
            
            $this->success($result);
            
        } catch (\Exception $e) {
            Logger::error('API Availability error', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'request' => $_GET
            ]);
            $this->error('Ошибка получения данных о наличии', 500);
        }
    }
    
    /**
     * GET /api/search - Поиск товаров
     */
    public function searchAction(): void
    {
        try {
            // Собираем и валидируем параметры
            $params = [
                'q' => trim($_GET['q'] ?? ''),
                'page' => max(1, (int)($_GET['page'] ?? 1)),
                'limit' => min(100, max(1, (int)($_GET['limit'] ?? 20))),
                'city_id' => max(1, (int)($_GET['city_id'] ?? 1)),
                'sort' => $_GET['sort'] ?? 'relevance'
            ];
            
            // Валидация сортировки
            $allowedSorts = ['relevance', 'name', 'external_id', 'price_asc', 'price_desc', 'availability', 'popularity'];
            if (!in_array($params['sort'], $allowedSorts)) {
                $params['sort'] = 'relevance';
            }
            
            // Добавляем пользователя если авторизован
            if (AuthService::check()) {
                $params['user_id'] = AuthService::user()['id'];
            }
            
            // Добавляем фильтры если они есть
            $filters = ['brand_name', 'series_name', 'category'];
            foreach ($filters as $filter) {
                if (!empty($_GET[$filter])) {
                    $params[$filter] = trim($_GET[$filter]);
                }
            }
            
            // Выполняем поиск
            $result = SearchService::search($params);
            
            // Логируем поиск для аналитики (только успешные)
            if (!empty($params['q'])) {
                Logger::info('Search performed', [
                    'query' => $params['q'],
                    'results_count' => $result['total'] ?? 0,
                    'city_id' => $params['city_id'],
                    'user_id' => $params['user_id'] ?? null
                ]);
            }
            
            $this->success($result);
            
        } catch (\Exception $e) {
            Logger::error('API Search error', [
                'error' => $e->getMessage(),
                'params' => $_GET
            ]);
            
            // Возвращаем пустой результат вместо ошибки чтобы не ломать UI
            $this->success([
                'products' => [],
                'total' => 0,
                'page' => (int)($_GET['page'] ?? 1),
                'limit' => (int)($_GET['limit'] ?? 20)
            ]);
        }
    }
    
    /**
     * GET /api/autocomplete - Автодополнение поиска
     */
    public function autocompleteAction(): void
    {
        try {
            $query = trim($_GET['q'] ?? '');
            $limit = min(20, max(1, (int)($_GET['limit'] ?? 10)));
            
            if (strlen($query) < 1) {
                $this->success(['suggestions' => []]);
                return;
            }
            
            // Убираем потенциально опасные символы
            $query = preg_replace('/[^\p{L}\p{N}\s\-_.]/u', '', $query);
            
            if (empty($query)) {
                $this->success(['suggestions' => []]);
                return;
            }
            
            $suggestions = SearchService::autocomplete($query, $limit);
            
            $this->success(['suggestions' => $suggestions]);
            
        } catch (\Exception $e) {
            Logger::warning('API Autocomplete error', [
                'error' => $e->getMessage(),
                'query' => $_GET['q'] ?? ''
            ]);
            
            // Не ломаем UI при ошибках автодополнения
            $this->success(['suggestions' => []]);
        }
    }
    
    /**
     * GET /api/test - Тестовый endpoint
     */
    public function testAction(): void
    {
        $this->success([
            'message' => 'API работает',
            'timestamp' => date('Y-m-d H:i:s'),
            'user_authenticated' => AuthService::check(),
            'opensearch_available' => $this->checkOpenSearchStatus()
        ]);
    }
    
    /**
     * Проверка статуса OpenSearch
     */
    private function checkOpenSearchStatus(): bool
    {
        try {
            $client = \OpenSearch\ClientBuilder::create()
                ->setHosts(['localhost:9200'])
                ->build();
            
            $info = $client->info();
            return isset($info['version']['number']);
        } catch (\Exception $e) {
            return false;
        }
    }
}