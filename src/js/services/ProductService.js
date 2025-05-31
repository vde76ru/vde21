/**
 * Централизованный сервис для работы с товарами
 * Версия 2.0 - с интеллектуальным поиском
 */
export class ProductService {
    constructor() {
        this.baseUrl = '/api';
        this.cache = new Map();
        this.cacheTimeout = 5 * 60 * 1000; // 5 минут
        this.requestTimeout = 10000; // 10 секунд
        this.searchDebounceTime = 300; // мс
        this.searchDebounceTimer = null;
        this.lastSearchQuery = '';
        this.searchHistory = this.loadSearchHistory();
    }
    
    /**
     * Универсальный поиск товаров с интеллектуальными функциями
     */
    async search(params = {}) {
        const endpoint = `${this.baseUrl}/search`;
        const allowedSorts = ['relevance', 'name', 'price_asc', 'price_desc', 'availability', 'popularity'];
        
        if (params.sort && !allowedSorts.includes(params.sort)) {
            params.sort = 'relevance';
        }
        
        // Нормализация поискового запроса
        if (params.q) {
            params.q = this.normalizeSearchQuery(params.q);
            this.saveToSearchHistory(params.q);
        }
        
        const cacheKey = this.getCacheKey('search', params);
        
        // Проверка кеша
        const cached = this.getFromCache(cacheKey);
        if (cached) return cached;
        
        try {
            const response = await this.request(endpoint, params);
            
            if (response.success) {
                const result = {
                    success: true,
                    data: {
                        products: response.data?.products || [],
                        total: response.data?.total || 0,
                        page: params.page || 1,
                        limit: params.limit || 20,
                        aggregations: response.data?.aggregations || {},
                        max_score: response.data?.max_score || 0,
                        suggestions: this.generateQuerySuggestions(params.q, response.data)
                    }
                };
                
                // Постобработка результатов
                this.enhanceSearchResults(result.data, params.q);
                
                this.saveToCache(cacheKey, result);
                return result;
            }
            
            return this.errorResponse('Search failed');
            
        } catch (error) {
            console.error('Search error:', error);
            return this.errorResponse(error.message);
        }
    }
    
    /**
     * Автодополнение с интеллектуальными предложениями
     */
    async autocomplete(query, limit = 10) {
        if (!query || query.length < 1) {
            // Показываем историю поиска если пусто
            return { 
                success: true, 
                suggestions: this.getSearchHistorySuggestions(limit) 
            };
        }
        
        query = this.normalizeSearchQuery(query);
        const endpoint = `${this.baseUrl}/autocomplete`;
        
        try {
            const response = await this.request(endpoint, { q: query, limit }, 3000);
            
            const suggestions = response.data?.suggestions || [];
            
            // Добавляем подсказки из истории
            const historySuggestions = this.searchHistory
                .filter(h => h.toLowerCase().includes(query.toLowerCase()))
                .slice(0, 3)
                .map(text => ({
                    text,
                    type: 'history',
                    score: 100
                }));
            
            // Объединяем и сортируем
            const allSuggestions = [...historySuggestions, ...suggestions]
                .sort((a, b) => (b.score || 0) - (a.score || 0))
                .slice(0, limit);
            
            return {
                success: true,
                suggestions: allSuggestions
            };
            
        } catch (error) {
            return { success: false, suggestions: [] };
        }
    }
    
    /**
     * Поиск с отложенным выполнением (debounce)
     */
    searchDebounced(params = {}) {
        return new Promise((resolve) => {
            clearTimeout(this.searchDebounceTimer);
            this.searchDebounceTimer = setTimeout(async () => {
                const result = await this.search(params);
                resolve(result);
            }, this.searchDebounceTime);
        });
    }
    
    /**
     * Получить товары по ID с кешированием
     */
    async getProductsByIds(ids, cityId = null) {
        if (!ids.length) return { success: true, data: [] };
        
        const endpoint = `${this.baseUrl}/products/batch`;
        const cacheKey = this.getCacheKey('batch', { ids: ids.sort(), cityId });
        
        const cached = this.getFromCache(cacheKey);
        if (cached) return cached;
        
        try {
            const response = await this.request(endpoint, {
                ids: ids.join(','),
                city_id: cityId || this.getCurrentCityId()
            });
            
            const result = {
                success: true,
                data: response.data || []
            };
            
            this.saveToCache(cacheKey, result);
            return result;
            
        } catch (error) {
            return this.errorResponse(error.message);
        }
    }
    
    /**
     * Получить один товар
     */
    async getProduct(id, cityId = null) {
        const products = await this.getProductsByIds([id], cityId);
        if (products.success && products.data.length > 0) {
            return {
                success: true,
                data: products.data[0]
            };
        }
        return this.errorResponse('Product not found');
    }
    
    /**
     * Нормализация поискового запроса
     */
    normalizeSearchQuery(query) {
        return query
            .trim()
            .replace(/\s+/g, ' ')           // Множественные пробелы
            .replace(/['"]/g, '')           // Удаляем кавычки
            .toLowerCase();
    }
    
    /**
     * Улучшение результатов поиска
     */
    enhanceSearchResults(data, query) {
        if (!query || !data.products.length) return;
        
        // Добавляем дополнительную информацию к результатам
        data.products.forEach(product => {
            // Подсчитываем релевантность для UI
            product._relevance = this.calculateRelevance(product, query);
            
            // Маркируем точные совпадения
            if (product.external_id?.toLowerCase() === query) {
                product._exact_match = true;
                product._match_type = 'external_id';
            } else if (product.sku?.toLowerCase() === query) {
                product._exact_match = true;
                product._match_type = 'sku';
            }
            
            // Форматируем подсветку
            if (product._highlight) {
                product._formatted_name = this.formatHighlight(
                    product.name, 
                    product._highlight.name?.[0] || product.name
                );
            }
        });
        
        // Группируем по типам совпадений для UI
        data.groups = this.groupSearchResults(data.products);
    }
    
    /**
     * Расчет релевантности для UI
     */
    calculateRelevance(product, query) {
        let score = product._score || 0;
        
        // Бонусы за различные факторы
        if (product.in_stock) score += 10;
        if (product.popularity_score) score += product.popularity_score * 5;
        if (product._exact_match) score += 100;
        
        // Штраф за длинное название
        if (product.name && product.name.length > 100) score -= 5;
        
        return Math.round(score);
    }
    
    /**
     * Форматирование подсветки
     */
    formatHighlight(original, highlighted) {
        // Заменяем теги подсветки на span с классом
        return highlighted
            .replace(/<mark>/g, '<span class="search-highlight">')
            .replace(/<\/mark>/g, '</span>');
    }
    
    /**
     * Группировка результатов поиска
     */
    groupSearchResults(products) {
        const groups = {
            exact: [],      // Точные совпадения
            high: [],       // Высокая релевантность
            medium: [],     // Средняя релевантность
            low: []         // Низкая релевантность
        };
        
        products.forEach(product => {
            if (product._exact_match) {
                groups.exact.push(product);
            } else if (product._relevance > 70) {
                groups.high.push(product);
            } else if (product._relevance > 30) {
                groups.medium.push(product);
            } else {
                groups.low.push(product);
            }
        });
        
        return groups;
    }
    
    /**
     * Генерация предложений по улучшению запроса
     */
    generateQuerySuggestions(query, searchData) {
        const suggestions = [];
        
        if (!query || searchData.total === 0) {
            suggestions.push({
                type: 'tip',
                text: 'Попробуйте изменить запрос или использовать другие слова'
            });
        }
        
        if (searchData.total < 5 && query.length > 3) {
            // Предлагаем более короткий запрос
            const words = query.split(' ');
            if (words.length > 1) {
                suggestions.push({
                    type: 'alternative',
                    text: `Попробуйте искать: "${words[0]}"`
                });
            }
        }
        
        return suggestions;
    }
    
    /**
     * История поиска
     */
    saveToSearchHistory(query) {
        if (!query || query.length < 2) return;
        
        // Удаляем дубликаты
        this.searchHistory = this.searchHistory.filter(q => q !== query);
        
        // Добавляем в начало
        this.searchHistory.unshift(query);
        
        // Ограничиваем размер
        if (this.searchHistory.length > 20) {
            this.searchHistory = this.searchHistory.slice(0, 20);
        }
        
        // Сохраняем в localStorage
        try {
            localStorage.setItem('searchHistory', JSON.stringify(this.searchHistory));
        } catch (e) {
            console.warn('Failed to save search history');
        }
    }
    
    loadSearchHistory() {
        try {
            const saved = localStorage.getItem('searchHistory');
            return saved ? JSON.parse(saved) : [];
        } catch (e) {
            return [];
        }
    }
    
    getSearchHistorySuggestions(limit) {
        return this.searchHistory
            .slice(0, limit)
            .map(text => ({
                text,
                type: 'history',
                score: 50
            }));
    }
    
    clearSearchHistory() {
        this.searchHistory = [];
        try {
            localStorage.removeItem('searchHistory');
        } catch (e) {}
    }
    
    /**
     * Универсальный метод запроса с улучшенной обработкой ошибок
     */
    async request(url, params = {}, timeout = null) {
        const controller = new AbortController();
        const requestId = this.generateRequestId();
        
        const timeoutId = setTimeout(
            () => controller.abort(),
            timeout || this.requestTimeout
        );
        
        try {
            // Санитизация параметров
            const cleanParams = this.sanitizeParams(params);
            const queryString = new URLSearchParams(cleanParams).toString();
            const fullUrl = queryString ? `${url}?${queryString}` : url;
            
            console.log(`🔍 [${requestId}] Request: ${fullUrl}`);
            
            const response = await fetch(fullUrl, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-Request-ID': requestId
                },
                credentials: 'same-origin',
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            // Специальная обработка разных HTTP статусов
            if (!response.ok) {
                await this.handleHttpError(response, fullUrl, requestId);
            }
            
            const data = await response.json();
            
            if (!data || typeof data !== 'object') {
                throw new Error('INVALID_RESPONSE_FORMAT');
            }
            
            console.log(`✅ [${requestId}] Success`);
            return data;
            
        } catch (error) {
            clearTimeout(timeoutId);
            return await this.handleRequestError(error, url, params, requestId);
        }
    }
    
    /**
     * Обработка HTTP ошибок
     */
    async handleHttpError(response, fullUrl, requestId) {
        const status = response.status;
        let errorDetails = `HTTP ${status}`;
        
        try {
            const errorBody = await response.text();
            if (errorBody) {
                errorDetails += `: ${errorBody.substring(0, 200)}`;
            }
        } catch (e) {
            // Игнорируем ошибки чтения body
        }
        
        console.error(`❌ [${requestId}] ${errorDetails}`);
        
        switch (status) {
            case 503:
                throw new Error('SERVICE_TEMPORARILY_UNAVAILABLE');
            case 502:
            case 504:
                throw new Error('GATEWAY_ERROR');
            case 429:
                throw new Error('RATE_LIMIT_EXCEEDED');
            case 400:
                throw new Error('BAD_REQUEST');
            default:
                throw new Error(`HTTP_${status}`);
        }
    }
    
    /**
     * Интеллектуальная обработка ошибок запроса
     */
    async handleRequestError(error, url, params, requestId) {
        console.error(`❌ [${requestId}] Error:`, error.message);
        
        // Специальная обработка для 503
        if (error.message === 'SERVICE_TEMPORARILY_UNAVAILABLE') {
            return await this.handle503Error(url, params, requestId);
        }
        
        if (error.name === 'AbortError') {
            return await this.handleTimeoutError(url, params, requestId);
        }
        
        // Общая fallback стратегия
        return this.getFallbackResponse(error, params);
    }
    
    /**
     * Специальная обработка 503 ошибок
     */
    async handle503Error(url, params, requestId) {
        console.warn(`⚠️ [${requestId}] Service unavailable, trying fallback strategies`);
        
        // Стратегия 1: Проверяем кеш
        const cacheKey = this.getCacheKey('search', params);
        const cached = this.getFromCache(cacheKey);
        
        if (cached) {
            console.log(`💾 [${requestId}] Using cache`);
            return {
                ...cached,
                warning: 'Показаны результаты из кеша - сервис поиска временно недоступен',
                fallback_used: 'cache'
            };
        }
        
        // Стратегия 2: Упрощенный запрос
        if (params.q && params.q.length > 5) {
            console.log(`🔄 [${requestId}] Trying simplified request`);
            
            try {
                const simplifiedParams = {
                    ...params,
                    q: params.q.substring(0, Math.min(10, params.q.length)),
                    limit: Math.min(params.limit || 20, 10)
                };
                
                // Даем еще одну попытку с упрощенными параметрами
                await new Promise(resolve => setTimeout(resolve, 1000)); // Пауза 1 сек
                
                const result = await this.makeSimpleRequest(url, simplifiedParams);
                return {
                    ...result,
                    warning: 'Показаны результаты упрощенного поиска',
                    fallback_used: 'simplified'
                };
                
            } catch (e) {
                console.warn(`⚠️ [${requestId}] Simplified request also failed`);
            }
        }
        
        // Стратегия 3: Возвращаем graceful fallback
        return this.getFallbackResponse(new Error('Service temporarily unavailable'), params);
    }
    
    /**
     * Упрощенный запрос
     */
    async makeSimpleRequest(url, params) {
        const controller = new AbortController();
        setTimeout(() => controller.abort(), 5000); // Короткий timeout
        
        const queryString = new URLSearchParams(params).toString();
        const fullUrl = `${url}?${queryString}`;
        
        const response = await fetch(fullUrl, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            signal: controller.signal
        });
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        return await response.json();
    }
    
    /**
     * Санитизация параметров
     */
    sanitizeParams(params) {
        const sanitized = {};
        
        Object.entries(params).forEach(([key, value]) => {
            if (value !== null && value !== undefined && value !== '') {
                switch (key) {
                    case 'q':
                        // Очищаем поисковый запрос
                        sanitized[key] = String(value)
                            .trim()
                            .replace(/[<>'"\\]/g, '')
                            .replace(/\s+/g, ' ')
                            .substring(0, 200);
                        break;
                    case 'page':
                    case 'limit':
                    case 'city_id':
                        const numValue = parseInt(value);
                        if (!isNaN(numValue) && numValue > 0) {
                            sanitized[key] = numValue;
                        }
                        break;
                    case 'sort':
                        const allowedSorts = ['relevance', 'name', 'price_asc', 'price_desc', 'availability', 'popularity'];
                        if (allowedSorts.includes(value)) {
                            sanitized[key] = value;
                        }
                        break;
                    default:
                        sanitized[key] = value;
                }
            }
        });
        
        return sanitized;
    }
    
    generateRequestId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
    }
    
    /**
     * Генерация ответа при ошибке
     */
    errorResponse(message) {
        return {
            success: false,
            error: message,
            data: {
                products: [],
                total: 0
            }
        };
    }
    
    /**
     * Предзагрузка популярных товаров
     */
    async preloadPopularProducts() {
        const params = {
            sort: 'popularity',
            limit: 20
        };
        
        try {
            await this.search(params);
        } catch (e) {
            console.warn('Failed to preload popular products');
        }
    }
}

// Экспорт синглтона
export const productService = new ProductService();

// Автоматическая предзагрузка при инициализации
if (typeof window !== 'undefined') {
    window.addEventListener('load', () => {
        productService.preloadPopularProducts();
    });
}