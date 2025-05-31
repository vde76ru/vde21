<?php
declare(strict_types=1);

// Критическая обработка ошибок
error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');

// Загружаем autoloader
require_once __DIR__ . '/../vendor/autoload.php';

// Минимальная инициализация
try {
    // 1. Config
    \App\Core\Config::get('app.name');
    
    // 2. Cache (опционально)
    if (class_exists('\App\Core\Cache')) {
        \App\Core\Cache::init();
    }
    
    // 3. Security headers
    if (class_exists('\App\Core\SecurityManager')) {
        \App\Core\SecurityManager::initialize();
    }
    
    // 4. Session
    \App\Core\Session::start();
    
    // 5. Database
    \App\Core\Database::getConnection();
    
    // 6. Logger (после БД)
    if (class_exists('\App\Core\Logger')) {
        \App\Core\Logger::initialize();
    }

} catch (\Exception $e) {
    error_log("Critical init error: " . $e->getMessage() . " in " . $e->getFile() . ":" . $e->getLine());
    http_response_code(500);
    die('System temporarily unavailable');
}

// Импорты контроллеров
use App\Core\Router;
use App\Controllers\LoginController;
use App\Controllers\AdminController;
use App\Controllers\CartController;
use App\Controllers\SpecificationController;
use App\Controllers\ProductController;
use App\Controllers\ApiController;

// Роутер
$router = new Router();

// API роуты
$apiController = new ApiController();
$router->get('/api/test', [$apiController, 'testAction']);
$router->get('/api/availability', [$apiController, 'availabilityAction']);
$router->get('/api/search', [$apiController, 'searchAction']);
$router->get('/api/autocomplete', [$apiController, 'autocompleteAction']);

// Авторизация
$loginController = new LoginController();
$router->match(['GET', 'POST'], '/login', [$loginController, 'loginAction']);

$router->get('/logout', function() {
    \App\Services\AuthService::destroySession();
    header('Location: /login');
    exit;
});

// Админ
$adminController = new AdminController();
$router->get('/admin', function() use ($adminController) {
    if (class_exists('\App\Middleware\AuthMiddleware')) {
        \App\Middleware\AuthMiddleware::requireRole('admin');
    }
    $adminController->indexAction();
});

// Корзина
$cartController = new CartController();
$router->match(['GET', 'POST'], '/cart/add', [$cartController, 'addAction']);
$router->get('/cart', [$cartController, 'viewAction']);
$router->post('/cart/clear', [$cartController, 'clearAction']);
$router->post('/cart/remove', [$cartController, 'removeAction']);
$router->get('/cart/json', [$cartController, 'getJsonAction']);

// Спецификации
$specController = new SpecificationController();
$router->match(['GET', 'POST'], '/specification/create', [$specController, 'createAction']);
$router->get('/specification/{id}', [$specController, 'viewAction']);
$router->get('/specifications', [$specController, 'listAction']);

// Товары
$productController = new ProductController();
$router->get('/shop/product', [$productController, 'viewAction']);
$router->get('/shop/product/{id}', [$productController, 'viewByIdAction']);
$router->get('/shop', function() {
    \App\Core\Layout::render('shop/index', []);
});

// Главная
$router->get('/', function() {
    \App\Core\Layout::render('home/index', []);
});

// 404
$router->set404(function() {
    http_response_code(404);
    \App\Core\Layout::render('errors/404', []);
});

// Обработка запроса
try {
    $router->dispatch();
} catch (\Exception $e) {
    error_log("Router error: " . $e->getMessage());
    
    if (strpos($_SERVER['REQUEST_URI'] ?? '', '/api/') !== false) {
        http_response_code(500);
        header('Content-Type: application/json');
        echo json_encode(['success' => false, 'error' => 'Internal server error']);
    } else {
        http_response_code(500);
        echo "Internal server error";
    }
}