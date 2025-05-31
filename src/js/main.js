import "../css/main.css";

import { loadPage, changeItemsPerPage, changePage, handlePageInputKeydown } from './pagination.js';
import { filterByBrandOrSeries, applyFilters, clearAllFilters } from './filters.js';
import { sortProducts } from './sort.js';
import { loadAvailability } from './availability.js';
import { addToCart, clearCart, removeFromCart, fetchCart } from './cart.js';
import { showToast, fetchProducts } from './utils.js';
import { renderProductsTable, copyText } from './renderProducts.js';
import { createSpecification } from './specification.js';
// ИЗМЕНЕНИЕ: Импортируем productService вместо smartSearch
import { productService } from './services/ProductService.js';

// Инициализация глобальных переменных
window.itemsPerPage = parseInt(sessionStorage.getItem('itemsPerPage') || '20', 10);
window.currentPage = 1;
window.productsData = [];
window.totalProducts = 0;
window.sortColumn = sessionStorage.getItem('sortColumn') || 'name';
window.sortDirection = sessionStorage.getItem('sortDirection') || 'asc';
window.appliedFilters = {};
window.cart = {};

// Восстановление фильтров из sessionStorage
Object.keys(sessionStorage).forEach(key => {
    if (!['itemsPerPage', 'sortColumn', 'sortDirection'].includes(key)) {
        window.appliedFilters[key] = sessionStorage.getItem(key);
    }
});

// Экспорт функций в window для обратной совместимости
window.renderProductsTable = renderProductsTable;
window.copyText = copyText;
window.createSpecification = createSpecification;
window.loadAvailability = loadAvailability;
window.addToCart = addToCart;
window.clearCart = clearCart;
window.removeFromCart = removeFromCart;
window.fetchCart = fetchCart;
window.filterByBrandOrSeries = filterByBrandOrSeries;
window.fetchProducts = fetchProducts;
window.sortProducts = sortProducts;
window.loadPage = loadPage;

// НОВОЕ: Класс для управления поиском и автодополнением
class SearchManager {
    constructor() {
        this.searchInput = null;
        this.globalSearchInput = null;
        this.autocompleteContainer = null;
        this.searchTimeout = null;
        this.selectedIndex = -1;
        this.isSearching = false;
    }

    init() {
        // Инициализация основного поиска на странице каталога
        this.searchInput = document.getElementById('searchInput');
        if (this.searchInput) {
            this.setupSearch(this.searchInput);
        }

        // Инициализация глобального поиска в хедере
        this.globalSearchInput = document.getElementById('globalSearch');
        if (this.globalSearchInput) {
            this.setupGlobalSearch(this.globalSearchInput);
        }
    }

    setupSearch(input) {
        // Создаем контейнер для автодополнения
        this.createAutocompleteContainer(input);

        // Обработчики событий
        input.addEventListener('input', (e) => this.handleSearchInput(e));
        input.addEventListener('focus', () => this.handleFocus());
        input.addEventListener('blur', () => {
            setTimeout(() => this.hideAutocomplete(), 200);
        });
        input.addEventListener('keydown', (e) => this.handleKeydown(e));

        // Восстанавливаем значение из фильтров
        if (window.appliedFilters.search) {
            input.value = window.appliedFilters.search;
        }
    }

    setupGlobalSearch(input) {
        // Для глобального поиска - переход на страницу каталога
        input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && input.value.trim()) {
                window.location.href = `/shop?search=${encodeURIComponent(input.value.trim())}`;
            }
        });
    }

    createAutocompleteContainer(input) {
        this.autocompleteContainer = document.createElement('div');
        this.autocompleteContainer.className = 'search-autocomplete';
        this.autocompleteContainer.style.cssText = `
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #ddd;
            border-top: none;
            border-radius: 0 0 4px 4px;
            max-height: 400px;
            overflow-y: auto;
            display: none;
            z-index: 1000;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        `;

        const parent = input.parentElement;
        parent.style.position = 'relative';
        parent.appendChild(this.autocompleteContainer);
    }

    async handleSearchInput(event) {
        const query = event.target.value.trim();

        // Сохраняем в фильтры
        if (query) {
            window.appliedFilters.search = query;
            sessionStorage.setItem('search', query);
        } else {
            delete window.appliedFilters.search;
            sessionStorage.removeItem('search');
        }

        // Отменяем предыдущий поиск
        clearTimeout(this.searchTimeout);
        
        // Автодополнение
        if (query.length >= 2 && !this.isSearching) {
            this.isSearching = true;
            try {
                const result = await productService.autocomplete(query);
                if (result.success) {
                    this.showAutocomplete(result.suggestions);
                }
            } catch (error) {
                console.error('Autocomplete error:', error);
            } finally {
                this.isSearching = false;
            }
        } else if (query.length < 2) {
            this.hideAutocomplete();
        }

        // Основной поиск с дебаунсом
        this.searchTimeout = setTimeout(() => {
            window.currentPage = 1;
            window.fetchProducts();
        }, 300);
    }

    showAutocomplete(suggestions) {
        if (!suggestions || suggestions.length === 0) {
            this.hideAutocomplete();
            return;
        }

        this.autocompleteContainer.innerHTML = '';
        this.selectedIndex = -1;

        suggestions.forEach((suggestion, index) => {
            const item = document.createElement('div');
            item.className = 'autocomplete-item';
            item.dataset.index = index;
            item.dataset.text = suggestion.text;
            item.style.cssText = `
                padding: 10px 15px;
                cursor: pointer;
                display: flex;
                justify-content: space-between;
                align-items: center;
                transition: background-color 0.2s;
            `;
            
            // Подсветка совпадений
            const highlightedText = this.highlightQuery(suggestion.text, this.searchInput.value);
            
            item.innerHTML = `
                <span>${highlightedText}</span>
                <span style="font-size: 0.85em; color: #666; margin-left: 10px;">
                    ${this.getTypeLabel(suggestion.type)}
                </span>
            `;

            // Hover эффект
            item.addEventListener('mouseenter', () => {
                item.style.backgroundColor = '#f5f5f5';
            });
            item.addEventListener('mouseleave', () => {
                item.style.backgroundColor = '';
            });

            // Клик по подсказке
            item.addEventListener('click', () => {
                this.selectAutocompleteItem(item);
            });

            this.autocompleteContainer.appendChild(item);
        });

        this.autocompleteContainer.style.display = 'block';
    }

    highlightQuery(text, query) {
        const regex = new RegExp(`(${this.escapeRegex(query)})`, 'gi');
        return text.replace(regex, '<strong>$1</strong>');
    }

    escapeRegex(str) {
        return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    getTypeLabel(type) {
        const labels = {
            product: 'Товар',
            code: 'Артикул',
            brand: 'Бренд',
            category: 'Категория'
        };
        return labels[type] || '';
    }

    selectAutocompleteItem(item) {
        const text = item.dataset.text;
        const externalId = item.dataset.externalId;
        this.searchInput.value = text;
        this.hideAutocomplete();
        
        // Если есть external_id - сразу переходим на товар
        if (externalId) {
            window.location.href = `/shop/product?id=${externalId}`;
        } else {
            window.appliedFilters.search = text;
            window.fetchProducts();
        }
    }


    hideAutocomplete() {
        if (this.autocompleteContainer) {
            this.autocompleteContainer.style.display = 'none';
            this.selectedIndex = -1;
        }
    }

    handleKeydown(event) {
        const items = this.autocompleteContainer?.querySelectorAll('.autocomplete-item') || [];
        
        switch(event.key) {
            case 'ArrowDown':
                event.preventDefault();
                this.selectedIndex = Math.min(this.selectedIndex + 1, items.length - 1);
                this.highlightItem(items);
                break;
                
            case 'ArrowUp':
                event.preventDefault();
                this.selectedIndex = Math.max(this.selectedIndex - 1, -1);
                this.highlightItem(items);
                break;
                
            case 'Enter':
                event.preventDefault();
                if (this.selectedIndex >= 0 && items[this.selectedIndex]) {
                    this.selectAutocompleteItem(items[this.selectedIndex]);
                } else {
                    window.currentPage = 1;
                    window.fetchProducts();
                }
                break;
                
            case 'Escape':
                this.hideAutocomplete();
                break;
        }
    }

    highlightItem(items) {
        items.forEach((item, index) => {
            if (index === this.selectedIndex) {
                item.style.backgroundColor = '#f5f5f5';
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.style.backgroundColor = '';
            }
        });
    }

    handleFocus() {
        if (this.searchInput.value.length >= 2) {
            // Показываем существующие подсказки при фокусе
            if (this.autocompleteContainer.children.length > 0) {
                this.autocompleteContainer.style.display = 'block';
            }
        }
    }
}

// Создаем экземпляр менеджера поиска
const searchManager = new SearchManager();

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Инициализация поиска
    searchManager.init();
    
    // Город
    const citySelect = document.getElementById('citySelect');
    if (citySelect) {
        citySelect.value = localStorage.getItem('selectedCityId') || '1';
        citySelect.addEventListener('change', () => {
            localStorage.setItem('selectedCityId', citySelect.value);
            // Очищаем кеш при смене города
            productService.clearCache();
            if (window.productsData.length > 0) {
                fetchProducts();
            }
        });
    }
    
    // Количество товаров на странице
    ['itemsPerPageSelect', 'itemsPerPageSelectBottom'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.value = window.itemsPerPage;
            el.addEventListener('change', changeItemsPerPage);
        }
    });
    
    // Ввод номера страницы
    ['pageInput', 'pageInputBottom'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', changePage);
            el.addEventListener('keydown', handlePageInputKeydown);
        }
    });
    
    // Выбрать все
    const selectAllEl = document.getElementById('selectAll');
    if (selectAllEl) {
        selectAllEl.addEventListener('change', event => {
            const isChecked = event.target.checked;
            document.querySelectorAll('.product-checkbox').forEach(checkbox => {
                checkbox.checked = isChecked;
            });
        });
    }
    
    // Обработчики кликов
    document.body.addEventListener('click', handleBodyClick);
    
    // Кнопки пагинации
    document.querySelectorAll('.prev-btn').forEach(btn => {
        btn.addEventListener('click', evt => {
            evt.preventDefault();
            loadPage(Math.max(1, window.currentPage - 1));
        });
    });
    
    document.querySelectorAll('.next-btn').forEach(btn => {
        btn.addEventListener('click', evt => {
            evt.preventDefault();
            const total = Math.ceil(window.totalProducts / window.itemsPerPage);
            loadPage(Math.min(total, window.currentPage + 1));
        });
    });
    
    // Загрузка товаров если мы на странице каталога
    if (document.querySelector('.product-table')) {
        loadPage(window.currentPage);
    }
    
    // Загрузка корзины
    if (document.querySelector('.cart-container') || document.getElementById('cartBadge')) {
        fetchCart().catch(console.error);
    }
});

// Обработчик кликов по body
function handleBodyClick(e) {
    const target = e.target;
    
    // Добавить в корзину
    if (target.closest('.add-to-cart-btn')) {
        const btn = target.closest('.add-to-cart-btn');
        const productId = btn.dataset.productId;
        const quantityInput = btn.closest('tr')?.querySelector('.quantity-input');
        const quantity = parseInt(quantityInput?.value || '1', 10);
        addToCart(productId, quantity);
        return;
    }
    
    // Удалить из корзины
    if (target.closest('.remove-from-cart-btn')) {
        const btn = target.closest('.remove-from-cart-btn');
        removeFromCart(btn.dataset.productId);
        return;
    }
    
    // Очистить корзину
    if (target.matches('#clearCartBtn') || target.closest('.clear-cart-btn')) {
        if (confirm('Очистить корзину?')) {
            clearCart();
        }
        return;
    }
    
    // Создать спецификацию
    if (target.closest('.create-specification-btn')) {
        e.preventDefault();
        createSpecification();
        return;
    }
    
    // Сортировка
    const sortableHeader = target.closest('th.sortable');
    if (sortableHeader && sortableHeader.dataset.column) {
        sortProducts(sortableHeader.dataset.column);
        return;
    }
}