/**
 * Calendar Picker Module
 * Padroniza comportamento de seletor de data em todas as páginas KPI
 *
 * Uso:
 * initCalendarPicker('#from', '#btnCalendarFrom', 'month'); // Month picker
 * initCalendarPicker('#dataIni', '#btnCalendarIni', 'day');   // Day picker
 */

function initCalendarPicker(inputSelector, buttonSelector, mode = 'month') {
    const input = document.querySelector(inputSelector);
    const button = document.querySelector(buttonSelector);

    if (!input || !button) return;

    // Remove focus event (se existir)
    input.removeEventListener('focus', openCalendar);

    // Adiciona click no botão
    button.addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        mostrarCalendario(input, mode);
    });

    // Permite digitar manualmente
    input.addEventListener('input', (e) => {
        let v = e.target.value.replace(/\D/g, '');
        if (mode === 'month') {
            if (v.length > 2) v = v.slice(0, 2) + '/' + v.slice(2, 6);
        } else if (mode === 'day') {
            if (v.length > 2) v = v.slice(0, 2) + '/' + v.slice(2, 4) + '/' + v.slice(4, 8);
        }
        e.target.value = v;
    });
}

/**
 * Abre o calendario modal
 * @param {HTMLElement} campo - Input element
 * @param {string} mode - 'month' ou 'day'
 */
function mostrarCalendario(campo, mode = 'month') {
    const existing = document.getElementById('pickerOverlay');
    if (existing) existing.remove();

    const overlay = document.createElement('div');
    overlay.id = 'pickerOverlay';
    overlay.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0, 0, 0, 0.4);
        z-index: 9999;
        display: flex;
        align-items: center;
        justify-content: center;
    `;

    const picker = document.createElement('div');
    picker.style.cssText = `
        position: relative;
        background: white;
        border-radius: 16px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.15);
        padding: 24px;
        z-index: 10000;
        min-width: 320px;
        max-width: 400px;
    `;

    const valor = campo.value.trim();
    let ano = new Date().getFullYear();
    let mes = new Date().getMonth() + 1;
    let dia = new Date().getDate();

    if (valor) {
        if (mode === 'month') {
            const match = valor.match(/^(\d{2})\/(\d{4})$/);
            if (match) {
                mes = Number(match[1]);
                ano = Number(match[2]);
            }
        } else if (mode === 'day') {
            const match = valor.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
            if (match) {
                dia = Number(match[1]);
                mes = Number(match[2]);
                ano = Number(match[3]);
            }
        }
    }

    const meses_nomes = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
                         'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'];

    let html = `
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                <button class="btnYear" style="padding: 8px 12px; cursor: pointer; background: #f0f0f0; border: none; border-radius: 8px; font-weight: 500;">◀</button>
                <span style="font-weight: 600; font-size: 18px; color: #1d1d1f;" id="yearDisplay">${ano}</span>
                <button class="btnYear" style="padding: 8px 12px; cursor: pointer; background: #f0f0f0; border: none; border-radius: 8px; font-weight: 500;">▶</button>
            </div>
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px;">
    `;

    for (let m = 1; m <= 12; m++) {
        const ativo = m === mes ? 'background: linear-gradient(135deg, #00dc64 0%, #006633 100%); color: white; font-weight: bold;' : 'background: #f5f5f7; color: #1d1d1f;';
        html += `
            <button type="button" class="btnMes" data-mes="${m}" style="padding: 10px; cursor: pointer; ${ativo} border: none; border-radius: 8px; font-weight: 500; transition: all 0.2s ease;">
                ${meses_nomes[m - 1].substring(0, 3)}
            </button>
        `;
    }

    html += `
            </div>
        </div>
    `;

    if (mode === 'day') {
        html += `
            <div style="text-align: center; margin-bottom: 20px; padding-top: 16px; border-top: 1px solid #e0e0e0;">
                <label style="display: block; margin-bottom: 8px; font-size: 0.75rem; font-weight: 600; color: #86868b; text-transform: uppercase;">Dia</label>
                <div style="display: grid; grid-template-columns: repeat(7, 1fr); gap: 4px;" id="diasContainer">
                </div>
            </div>
        `;
    }

    html += `
        <div style="text-align: right; margin-top: 20px; padding-top: 16px; border-top: 1px solid #e0e0e0; display: flex; gap: 8px; justify-content: flex-end;">
            <button class="btnCancel" style="padding: 8px 16px; cursor: pointer; background: white; border: 1px solid #e0e0e0; border-radius: 8px; font-weight: 500; color: #1d1d1f;">Cancelar</button>
            <button class="btnOk" style="padding: 8px 16px; cursor: pointer; background: linear-gradient(135deg, #00dc64 0%, #006633 100%); color: white; border: none; border-radius: 8px; font-weight: 500; box-shadow: 0 2px 8px rgba(0, 220, 100, 0.2);">OK</button>
        </div>
    `;

    picker.innerHTML = html;
    overlay.appendChild(picker);
    document.body.appendChild(overlay);

    let selectedMes = mes;
    let selectedAno = ano;
    let selectedDia = dia;

    // Eventos para meses
    picker.querySelectorAll('.btnMes').forEach(btn => {
        btn.addEventListener('click', function() {
            picker.querySelectorAll('.btnMes').forEach(b => {
                b.style.background = '#f5f5f7';
                b.style.color = '#1d1d1f';
                b.style.fontWeight = 'normal';
            });
            this.style.background = 'linear-gradient(135deg, #00dc64 0%, #006633 100%)';
            this.style.color = 'white';
            this.style.fontWeight = 'bold';
            selectedMes = Number(this.dataset.mes);

            if (mode === 'day') {
                renderDias(selectedAno, selectedMes, dia);
            }
        });
    });

    // Eventos para anos
    picker.querySelectorAll('.btnYear').forEach((btn, idx) => {
        btn.addEventListener('click', function() {
            if (idx === 0) selectedAno--;
            else selectedAno++;
            picker.querySelector('#yearDisplay').textContent = selectedAno;

            if (mode === 'day') {
                renderDias(selectedAno, selectedMes, dia);
            }
        });
    });

    // Renderizar dias se mode = 'day'
    if (mode === 'day') {
        renderDias(selectedAno, selectedMes, dia);

        function renderDias(y, m, d) {
            const diasContainer = picker.querySelector('#diasContainer');
            if (!diasContainer) return;

            const diasNoMes = new Date(y, m, 0).getDate();
            diasContainer.innerHTML = '';

            for (let i = 1; i <= diasNoMes; i++) {
                const ativo = i === d && selectedMes === mes && selectedAno === ano ? 'background: linear-gradient(135deg, #00dc64 0%, #006633 100%); color: white; font-weight: bold;' : 'background: #f5f5f7; color: #1d1d1f;';
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.textContent = i;
                btn.setAttribute('data-dia', i);
                btn.style.cssText = `padding: 8px; cursor: pointer; ${ativo} border: none; border-radius: 6px; font-weight: 500; transition: all 0.2s ease;`;
                btn.addEventListener('click', function() {
                    selectedDia = Number(this.dataset.dia);
                    picker.querySelectorAll('[data-dia]').forEach(b => {
                        b.style.background = '#f5f5f7';
                        b.style.color = '#1d1d1f';
                        b.style.fontWeight = 'normal';
                    });
                    this.style.background = 'linear-gradient(135deg, #00dc64 0%, #006633 100%)';
                    this.style.color = 'white';
                    this.style.fontWeight = 'bold';
                });
                diasContainer.appendChild(btn);
            }
        }
    }

    // OK button
    picker.querySelector('.btnOk').addEventListener('click', function() {
        if (mode === 'month') {
            const mesStr = String(selectedMes).padStart(2, '0');
            campo.value = `${mesStr}/${selectedAno}`;
        } else if (mode === 'day') {
            const diaStr = String(selectedDia).padStart(2, '0');
            const mesStr = String(selectedMes).padStart(2, '0');
            campo.value = `${diaStr}/${mesStr}/${selectedAno}`;
        }
        overlay.remove();
    });

    // Cancel button
    picker.querySelector('.btnCancel').addEventListener('click', function() {
        overlay.remove();
    });

    // Fechar ao clicar fora
    overlay.addEventListener('click', function(e) {
        if (e.target === this) overlay.remove();
    });
}
