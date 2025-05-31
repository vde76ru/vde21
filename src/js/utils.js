import { productService } from './services/ProductService.js';

export function showToast(message, isError = false) {
    const toast = document.createElement('div');
    toast.className = `toast ${isError ? 'toast-error' : 'toast-success'} show`;
    toast.innerHTML = `
        <div class="toast-content">
            <div class="toast-message">${message}</div>
        </div>
    `;
    
    const container = document.getElementById('toastContainer') || document.body;
    container.appendChild(toast);
    
    setTimeout(() => toast.remove(), 3000);
}

export async function fetchProducts() {
    showLoadingIndicator();
    
    try {
        // Собираем параметры для поиска
        const params = {
            q: window.appliedFilters.search || '',
            page: window.currentPage || 1,
            limit: window.itemsPerPage || 20,
            sort: convertSortToApiFormat(window.sortColumn, window.sortDirection),
            city_id: document.getElementById('citySelect')?.value || '1'
        };
        
        // Добавляем остальные фильтры
        Object.entries(window.appliedFilters).forEach(([key, value]) => {
            if (key !== 'search' && value) {
                params[key] = value;
            }
        });
        
        // Используем productService для поиска
        const result = await productService.search(params);
        
        if (result.success) {
            window.productsData = result.data.products;
            window.totalProducts = result.data.total;
            window.renderProductsTable();
            updatePaginationInfo();
            
            // Загружаем данные о наличии для отображенных товаров
            if (window.productsData.length > 0) {
                const ids = window.productsData.map(p => p.product_id);
                window.loadAvailability(ids);
            }
        } else {
            throw new Error(result.error || 'Ошибка загрузки');
        }
    } catch (error) {
        console.error('Fetch error:', error);
        showToast('Ошибка загрузки товаров', true);
        window.productsData = [];
        window.totalProducts = 0;
        window.renderProductsTable();
    } finally {
        hideLoadingIndicator();
    }
}

// Преобразование формата сортировки для API
function convertSortToApiFormat(column, direction) {
    // Специальные случаи для сортировки по цене
    if (column === 'base_price' || column === 'price') {
        return direction === 'asc' ? 'price_asc' : 'price_desc';
    }
    
    // Для остальных колонок возвращаем как есть
    const sortableColumns = ['name', 'external_id', 'sku', 'availability', 'popularity'];
    if (sortableColumns.includes(column)) {
        return column;
    }
    
    // По умолчанию - релевантность
    return 'relevance';
}

function updatePaginationInfo() {
    const totalPages = Math.ceil(window.totalProducts / window.itemsPerPage);
    
    // Обновляем все элементы пагинации
    document.querySelectorAll('#currentPage, #currentPageBottom').forEach(el => {
        if (el) el.textContent = window.currentPage;
    });
    
    document.querySelectorAll('#totalPages, #totalPagesBottom').forEach(el => {
        if (el) el.textContent = totalPages;
    });
    
    document.querySelectorAll('#totalProductsText, #totalProductsTextBottom').forEach(el => {
        if (el) el.textContent = `Найдено товаров: ${window.totalProducts}`;
    });
    
    // Управление состоянием кнопок пагинации
    document.querySelectorAll('.prev-btn').forEach(btn => {
        if (btn) btn.disabled = window.currentPage <= 1;
    });
    
    document.querySelectorAll('.next-btn').forEach(btn => {
        if (btn) btn.disabled = window.currentPage >= totalPages;
    });
}

export function showLoadingIndicator() {
    const existing = document.querySelector('.loading-indicator');
    if (existing) return;
    
    const indicator = document.createElement('div');
    indicator.className = 'loading-indicator';
    indicator.innerHTML = `
        <div class="spinner-border spinner-border-sm"></div>
        <span>Загрузка...</span>
    `;
    indicator.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: rgba(0, 0, 0, 0.8);
        color: white;
        padding: 10px 20px;
        border-radius: 4px;
        display: flex;
        align-items: center;
        gap: 10px;
        z-index: 9999;
    `;
    document.body.appendChild(indicator);
}

export function hideLoadingIndicator() {
    const indicator = document.querySelector('.loading-indicator');
    if (indicator) {
        indicator.remove();
    }
}