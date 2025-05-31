import { filterByBrandOrSeries, renderAppliedFilters, highlightFilteredWords } from "./filters.js";
import { loadAvailability } from "./availability.js";
import { showToast } from "./utils.js";

export function copyText(text) {
    if (!text) {
        showToast('Нечего копировать', true);
        return;
    }
    if (!navigator.clipboard) {
        showToast('Clipboard API не поддерживается', true);
        return;
    }
    navigator.clipboard.writeText(text)
        .then(() => showToast(`Скопировано: ${text}`))
        .catch(() => showToast('Не удалось скопировать', true));
}

export function bindSortableHeaders() {
    const table = document.querySelector('.product-table');
    if (!table) return;
    table.removeEventListener('click', sortableClickHandler);
    table.addEventListener('click', sortableClickHandler);
}

function sortableClickHandler(e) {
    const th = e.target.closest('th.sortable');
    if (th && window.sortProducts) window.sortProducts(th.dataset.column);
}

export function renderProductsTable() {
    const tbody = document.querySelector('.product-table tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    const fragment = document.createDocumentFragment();
    window.productsData.forEach(product => {
        fragment.appendChild(createProductRow(product));
    });
    tbody.appendChild(fragment);

    updateUI();
    loadMissingAvailability();
    initializeColResizable();
}

function createProductRow(product) {
    const row = document.createElement('tr');
    row.setAttribute('data-product-id', product.product_id);

    row.appendChild(createSelectCell(product));
    row.appendChild(createCodeCell(product));
    row.appendChild(createImageCell(product));
    row.appendChild(createNameCell(product));
    row.appendChild(createSkuCell(product));
    row.appendChild(createBrandSeriesCell(product));
    row.appendChild(createStatusCell(product));
    row.appendChild(createMinSaleUnitCell(product));
    row.appendChild(createAvailabilityCell(product));
    row.appendChild(createDeliveryDateCell(product));
    row.appendChild(createPriceCell(product));
    row.appendChild(createRetailPriceCell(product));
    row.appendChild(createCartCell(product));
    row.appendChild(createAdditionalFieldsCell());
    row.appendChild(createOrdersCountCell(product));

    return row;
}

function createSelectCell(product) {
    const cell = document.createElement('td');
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.classList.add('product-checkbox');
    cell.appendChild(checkbox);
    return cell;
}

function createCodeCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-code');
    const codeItem = document.createElement('div');
    codeItem.className = 'item-code';
    const codeSpan = document.createElement('span');
    codeSpan.textContent = product.external_id || '';
    codeItem.appendChild(codeSpan);
    const copyIcon = createCopyIcon(product.external_id);
    codeItem.appendChild(copyIcon);
    cell.appendChild(codeItem);
    return cell;
}

function createImageCell(product) {
    const cell = document.createElement('td');
    const urls = getProductImages(product);
    const firstUrl = urls[0] || '/images/placeholder.jpg';
    const container = document.createElement('div');
    container.className = 'image-container';
    container.style.position = 'relative';
    const thumb = document.createElement('img');
    thumb.src = firstUrl;
    thumb.alt = product.name || '';
    thumb.style.width = '50px';
    thumb.style.cursor = 'pointer';
    thumb.style.transition = 'opacity 0.3s ease';
    const zoom = document.createElement('img');
    zoom.className = 'zoom-image';
    zoom.src = firstUrl;
    zoom.alt = product.name || '';
    zoom.style.width = '350px';
    zoom.style.position = 'absolute';
    zoom.style.top = '0';
    zoom.style.left = '60px';
    zoom.style.opacity = '0';
    zoom.style.transition = 'opacity 0.3s ease';
    zoom.style.pointerEvents = 'none';
    zoom.style.zIndex = '1000';
    zoom.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
    zoom.style.backgroundColor = 'white';
    zoom.style.padding = '5px';
    zoom.style.border = '1px solid #ddd';
    zoom.style.borderRadius = '4px';

    thumb.addEventListener('mouseenter', () => {
        zoom.style.opacity = '1';
        zoom.style.pointerEvents = 'auto';
    });
    thumb.addEventListener('mouseleave', () => {
        zoom.style.opacity = '0';
        zoom.style.pointerEvents = 'none';
    });
    container.appendChild(thumb);
    container.appendChild(zoom);
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id}`;
    link.appendChild(container);
    cell.appendChild(link);
    return cell;
}

function createNameCell(product) {
    const cell = document.createElement('td');
    cell.className = 'name-cell';
    const link = document.createElement('a');
    link.href = `/shop/product?id=${product.external_id}`;
    link.style.color = 'inherit';
    link.style.textDecoration = 'none';
    const nameItem = document.createElement('div');
    nameItem.className = 'item-code';
    const nameSpan = document.createElement('span');

    if (product._highlight && product._highlight.name) {
        nameSpan.innerHTML = product._highlight.name[0];
    } else {
        nameSpan.textContent = product.name || '';
    }

    nameItem.appendChild(nameSpan);
    const copyIcon = createCopyIcon(product.name);
    nameItem.appendChild(copyIcon);
    link.appendChild(nameItem);
    cell.appendChild(link);
    return cell;
}

function createSkuCell(product) {
    const cell = document.createElement('td');
    const skuItem = document.createElement('div');
    skuItem.className = 'item-code';
    const skuSpan = document.createElement('span');
    skuSpan.textContent = product.sku || '';
    skuItem.appendChild(skuSpan);
    const copyIcon = createCopyIcon(product.sku);
    skuItem.appendChild(copyIcon);
    cell.appendChild(skuItem);
    return cell;
}

function createBrandSeriesCell(product) {
    const cell = document.createElement('td');
    const div = document.createElement('div');
    const brandSpan = document.createElement('span');
    brandSpan.className = 'brand-name';
    brandSpan.textContent = product.brand_name || '';
    brandSpan.style.cursor = 'pointer';
    brandSpan.addEventListener('click', () => filterByBrandOrSeries('brand_name', product.brand_name));
    const seriesSpan = document.createElement('span');
    seriesSpan.className = 'series-name';
    seriesSpan.textContent = product.series_name || '';
    seriesSpan.style.cursor = 'pointer';
    seriesSpan.addEventListener('click', () => filterByBrandOrSeries('series_name', product.series_name));
    if (brandSpan.textContent && seriesSpan.textContent) {
        brandSpan.textContent += ' / ';
    }
    div.appendChild(brandSpan);
    div.appendChild(seriesSpan);
    cell.appendChild(div);
    return cell;
}

function createStatusCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = product.status || 'Активен';
    cell.appendChild(span);
    return cell;
}

function createMinSaleUnitCell(product) {
    const cell = document.createElement('td');
    const minSaleSpan = document.createElement('span');
    minSaleSpan.textContent = product.min_sale || '';
    const unitSpan = document.createElement('span');
    unitSpan.textContent = product.unit ? ` / ${product.unit}` : '';
    cell.appendChild(minSaleSpan);
    cell.appendChild(unitSpan);
    return cell;
}

function createAvailabilityCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-availability');
    const span = document.createElement('span');
    if (product.stock) {
        const qty = product.stock.quantity || 0;
        span.textContent = qty > 0 ? `${qty} шт.` : "Нет";
        span.classList.toggle('in-stock', qty > 0);
        span.classList.toggle('out-of-stock', qty === 0);
    } else {
        span.textContent = '…';
    }
    cell.appendChild(span);
    return cell;
}

function createDeliveryDateCell(product) {
    const cell = document.createElement('td');
    cell.classList.add('col-delivery-date');
    const span = document.createElement('span');
    if (product.delivery) {
        span.textContent = product.delivery.date || product.delivery.text || '—';
    } else {
        span.textContent = '…';
    }
    cell.appendChild(span);
    return cell;
}

function createPriceCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    if (product.price && product.price.final) {
        span.textContent = `${product.price.final.toFixed(2)} руб.`;
        if (product.price.has_special) {
            span.innerHTML = `<span class="price-current">${product.price.final.toFixed(2)} руб.</span>`;
        }
    } else if (product.base_price) {
        span.textContent = `${product.base_price.toFixed(2)} руб.`;
    } else {
        span.textContent = 'Нет цены';
    }
    cell.appendChild(span);
    cell.setAttribute('data-fulltext', span.textContent);
    return cell;
}

function createRetailPriceCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    if (product.price && product.price.base && product.price.has_special) {
        span.innerHTML = `<span class="price-old">${product.price.base.toFixed(2)} руб.</span>`;
    } else if (product.retail_price) {
        span.textContent = `${product.retail_price.toFixed(2)} руб.`;
    } else {
        span.textContent = '—';
    }
    cell.appendChild(span);
    cell.setAttribute('data-fulltext', span.textContent);
    return cell;
}

function createCartCell(product) {
    const cell = document.createElement('td');
    const input = document.createElement('input');
    input.className = 'form-control quantity-input';
    input.type = 'number';
    input.value = 1;
    input.min = 1;
    const button = document.createElement('button');
    button.className = 'add-to-cart-btn';
    button.innerHTML = '<i class="fas fa-shopping-cart"></i>';
    button.dataset.productId = product.product_id;
    cell.appendChild(input);
    cell.appendChild(button);
    return cell;
}

function createAdditionalFieldsCell() {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = 'Доп. информация';
    cell.appendChild(span);
    return cell;
}

function createOrdersCountCell(product) {
    const cell = document.createElement('td');
    const span = document.createElement('span');
    span.textContent = product.orders_count || '0';
    cell.appendChild(span);
    return cell;
}

function createCopyIcon(text) {
    const icon = document.createElement('a');
    icon.className = 'copy-icon js-copy-to-clipboard';
    icon.href = '#';
    icon.setAttribute('data-text-to-copy', text || '');
    icon.innerHTML = '<i class="far fa-clone"></i>';
    icon.addEventListener('click', e => {
        e.preventDefault();
        copyText(icon.getAttribute('data-text-to-copy'));
    });
    return icon;
}

function getProductImages(product) {
    let urls = [];
    if (product.images && Array.isArray(product.images)) {
        urls = product.images;
    } else if (typeof product.image_urls === 'string' && product.image_urls.trim()) {
        urls = product.image_urls.split(',').map(u => u.trim());
    }
    return urls;
}

function updateUI() {
    if (typeof window.updatePaginationDisplay === "function") window.updatePaginationDisplay();
    if (typeof window.renderAppliedFilters === "function") window.renderAppliedFilters();
    if (typeof window.highlightFilteredWords === "function") window.highlightFilteredWords();
}

function loadMissingAvailability() {
    const productsNeedingAvailability = window.productsData.filter(p => !p.stock && !p.delivery);
    if (productsNeedingAvailability.length > 0) {
        const ids = productsNeedingAvailability.map(p => p.product_id);
        loadAvailability(ids);
    }
}

function initializeColResizable() {
    try {
        if (typeof jQuery !== 'undefined' && jQuery.fn.colResizable) {
            const $table = jQuery('#productTable');
            if ($table.length > 0) {
                $table.colResizable('destroy');
                $table.colResizable({
                    liveDrag: true,
                    minWidth: 30,
                    hoverCursor: "col-resize"
                });
            }
        }
    } catch (e) {
        console.warn('colResizable не инициализирован:', e.message);
    }
}