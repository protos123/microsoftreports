# -*- coding: utf-8 -*-
import sys
import os
import psycopg2 as db
import logging
import pandas as pd
import xlrd
import openpyxl
import datetime as dt
import numpy as np


# Intentar Conexion a BD
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='10.50.49.27', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

# Reportes Para Cuenta de Brazil
def createmonthlyreportforbrazil(first_date,last_date):
    #%(first_date)s
    cursor.execute("""WITH 
movement_values AS (
-- Selecciona todos los movimientos, modifica el valor ( * -1) según el tipo de operacion (NC - Crédito, ND - Débito)
SELECT 
m.cuenta_id,
c.nombre,
c.moneda_iso_4217,
m.documento_soporte, 
m.tipo_documento_soporte, 
m.tipo_movimiento, 
m.descripcion, 
m.saldo_anterior, 
saldo_congelado_anterior,
saldo_reserva_anterior,
m.fecha_creacion,
CASE
WHEN transaccion_relacionada_id IS NULL 
THEN documento_soporte
ELSE transaccion_relacionada_id END AS transaccion_relacionada_id,
CASE 
WHEN m.operacion = 'NC' THEN m.valor
ELSE m.valor * -1 
END AS valor
FROM pps.movimiento m 
INNER JOIN pps.cuenta c ON c.cuenta_id = m.cuenta_id
WHERE m.fecha_creacion BETWEEN %(first_date)s AND %(last_date)s AND m.cuenta_id = 677196
),
transposed_movements AS (
-- Transpone los valores de los movimientos agrupandolos según el tipo de movimiento
SELECT
cuenta_id, nombre, documento_soporte, transaccion_relacionada_id, tipo_documento_soporte AS tm_tipo_documento_soporte,
MIN(fecha_creacion) fecha_saldo_anterior,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN fecha_creacion ELSE NULL END) AS fecha_creacion,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN valor ELSE NULL END) AS tm_valor,
MAX(CASE WHEN tipo_movimiento IN ('POL_COMMISSION', 'PAYMENT_ORDER_POL_COMMISION', 'PAYMENT_ORDER_POL_COMMISION_SUPPLIER', 'ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE NULL END) AS tm_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('IVA_POL_COMMISSION', 'IVA_PAYMENT_ORDER_POL_COMMISION', 'IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER','IVA_ACCOUNT_TRANSFER_COMMISSION') THEN valor ELSE NULL END) AS tm_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento = 'ADDITIONAL_WITHHOLDING_TAX' THEN valor ELSE NULL END) AS tm_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento = 'ICA_RETENTION' THEN valor ELSE NULL END) AS tm_ica_retention,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_merchant_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iibb_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYER_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iva_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IVA_PAYER_COMMISSION' THEN valor ELSE NULL END) AS tm_iva_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_RETENTION' THEN valor ELSE NULL END) AS tm_iva_retention,
MAX(CASE WHEN tipo_movimiento = 'MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'PAYER_INTEREST' THEN valor ELSE NULL END) AS tm_payer_interest,
MAX(CASE WHEN tipo_movimiento = 'MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE NULL END) AS tm_release_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'RENTA_RETENTION' THEN valor ELSE NULL END) AS tm_renta_retention,
MAX(CASE WHEN tipo_movimiento = 'RESERVE_FUND' THEN valor ELSE NULL END) AS tm_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'TAX_MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_tax_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'TAX_POL_COMMISSION' THEN valor ELSE NULL END) AS tm_tax_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYMENT_ORDER_POL_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payment_order_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'PAYMENT_ORDER_REVERSION' AND descripcion ILIKE concat(tipo_movimiento,'%%IIBB_PAYMENT_ORDER_POL_COMMISION%%') THEN valor ELSE NULL END) AS tm_iibb_payment_order_reversion_pol_commission,
SUM(CASE WHEN tipo_movimiento in ('TAX_LAW_25413_DEBIT', 'TAX_LAW_25413_CREDIT', 'IIBB_ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE 0 END) AS tm_transfer_retention,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN fecha_creacion ELSE NULL END) AS tm_reversion_fecha,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN valor ELSE NULL END) AS tm_reversion_valor,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_POL_COMMISSION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteiva,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ICA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteica,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' RENTA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reterenta,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ADDITIONAL_WITHHOLDING_TAX%%') THEN valor ELSE NULL END) AS tm_reversion_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_PAYER_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_pagador,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iva_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_merchant_reversion_intereses,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' PAYER_INTEREST%%') THEN valor ELSE NULL END) AS tm_payer_reversion_intereses,
MAX(CASE WHEN tipo_movimiento = 'CHARGEBACK_FEE' THEN valor ELSE NULL END) AS tm_chargeback_fee,
SUM(CASE WHEN tipo_movimiento NOT IN('FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND', 'RESERVE_FUND')  THEN valor ELSE NULL END) as valor
FROM 
movement_values
GROUP BY 
1, 2, 3, 4, 5
),
movement_prev_balance AS (
-- Obtiene el balance previo tomado a partir de la fecha obtenida en la consulta transposed_movements.
SELECT m.documento_soporte, m.transaccion_relacionada_id, tm_tipo_documento_soporte, m.tipo_movimiento, saldo_anterior,saldo_congelado_anterior,saldo_reserva_anterior,moneda_iso_4217 
FROM transposed_movements t 
INNER JOIN  movement_values m on t.documento_soporte = m.documento_soporte AND t.fecha_saldo_anterior = m.fecha_creacion and t.cuenta_id = m.cuenta_id
),
transactions AS (               
--Obtiene las transacciones que tienen movimiento_soporte tipo ORDEN y tipo aut&capt y  captura
SELECT                      
t.orden_id,
t.transaccion_id,
t.convenio_id,
t.tipo,
t.transaccion_padre_id,
t.codigo_autorizacion,
t.fecha_creacion,
t.pagador_email,
t.pagador_nombre_completo,
t.pagador_numero_identificacion,
t.pagador_telefono_contacto,
t.pagador_direccion_cobro_pais,
t.pagador_direccion_cobro_calle1,
t.tarjeta_credito_id
FROM pps.transaccion t 
INNER JOIN 
(
SELECT DISTINCT transaccion_relacionada_id 
FROM movement_values 
WHERE tipo_documento_soporte in ('ORDER', 'REVERSION')
) mv ON t.transaccion_id = mv.transaccion_relacionada_id),
generic_sales_table AS (
-- Cruza los movientos de ventas con las tablas de transacciones para obtener los valores originales
SELECT 
tm.*,
pb.saldo_reserva_anterior,
pb.saldo_congelado_anterior,
TO_CHAR(tm.fecha_creacion, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
tm.fecha_creacion AS tm_fecha_creacion,
t.tipo AS t_tipo,
t.transaccion_padre_id,
t.transaccion_id AS transaction_id,
t.fecha_creacion AS sales_date,
o.cuenta_id AS account_id,
o.referencia AS reference,
o.descripcion AS description,
o.comprador_email,
o.direccion_envio_pais, 
o.direccion_envio_calle1, 
o.comprador_direccion_pais, 
o.direccion_envio_telefono, 
o.comprador_nombre_completo, 
o.comprador_direccion_calle1,
o.comprador_telefono_contacto, 
o.comprador_direccion_telefono,
o.comprador_numero_identificacion,
c.tipo_medio_pago AS payment_method,
tc.nombre AS tc_nombre,
pb.saldo_anterior AS pb_saldo_anterior,
pb.tipo_movimiento AS pb_tipo_movimiento,
CAST(o.orden_id AS varchar) AS order_id,
COALESCE(pm.nombre, '') AS promotion,
COALESCE(tc.numero_visible, '') AS credit_card_number,
COALESCE(tpe.installments_number, '') AS installments,
COALESCE(t.codigo_autorizacion, '') AS authorization_code,
COALESCE(pb.moneda_iso_4217, '') AS operation_currency,
COALESCE(t.pagador_email, o.comprador_email, '') AS t_payer_mail,
COALESCE(to_char(tm.tm_valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(t.pagador_nombre_completo, o.comprador_nombre_completo, tc.nombre, '') AS t_payer_name,
COALESCE(t.pagador_numero_identificacion, o.comprador_numero_identificacion, '') AS t_payer_document_number,
REPLACE(COALESCE(o.direccion_envio_pais, o.comprador_direccion_pais, t.pagador_direccion_cobro_pais, ''), ';', ',') AS t_payer_country,
REPLACE(COALESCE(o.direccion_envio_calle1, o.comprador_direccion_calle1, t.pagador_direccion_cobro_calle1, ''), ';', ',') AS t_payer_address,
REPLACE(COALESCE(t.pagador_telefono_contacto, o.comprador_telefono_contacto, o.direccion_envio_telefono, o.comprador_direccion_telefono, ''), ';', ',') AS t_payer_contact_phone
FROM transactions t
INNER JOIN transposed_movements tm ON tm.transaccion_relacionada_id = t.transaccion_id
INNER JOIN movement_prev_balance pb on pb.transaccion_relacionada_id = t.transaccion_id 
INNER JOIN pps.orden o ON o.orden_id = t.orden_id
INNER JOIN pps.convenio c ON c.convenio_id = t.convenio_id
LEFT JOIN pps.tarjeta_credito tc ON t.tarjeta_credito_id = tc.tarjeta_credito_id
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = t.transaccion_id 
LEFT JOIN pps.transaccion_datos_extra tpe ON tpe.transaccion_id = t.transaccion_id 
LEFT JOIN pps.promocion pm ON pm.promocion_id = CAST(tpe.promotion_id AS INT)
),
payment_orders AS (
-- Cruza los movimientos con las ordenes de pago para obtener los valores originales
SELECT op.* FROM pps.orden_pago op
INNER JOIN movement_values m ON m.documento_soporte = CAST(op.orden_pago_id AS varchar) AND m.tipo_documento_soporte = 'PAYMENT_ORDER'
WHERE op.cuenta_id = m.cuenta_id
),
payment_order_net_amount AS (
-- Transpone los valores de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER'
AND m.tipo_movimiento NOT IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
),
payments_order_table AS (
-- Obtiene los valores originales de la orden de pago
SELECT 
tm.fecha_creacion AS operation_date,
CAST(op.cuenta_id AS int) AS account_id,
tm.nombre,
op.tipo_transferencia_orden_pago,
op.orden_pago_id,
pb.tipo_movimiento AS pb_tipo_movimiento,
COALESCE(moneda_iso_4217, '') AS operation_currency,
tm_valor,
tm_comision_payu,
tm_iva_comision_payu,
tm_reversion_valor,
tm_reversion_comision_payu,
tm_reversion_iva_comision_payu,
tm_tipo_documento_soporte,
tm_iibb_payment_order_pol_commission,
tm_iibb_payment_order_reversion_pol_commission,
pb.saldo_anterior AS pb_saldo_anterior,
saldo_congelado_anterior,
saldo_reserva_anterior
FROM
payment_orders op
INNER JOIN transposed_movements tm ON tm.documento_soporte = CAST(op.orden_pago_id AS varchar) AND tm.cuenta_id = op.cuenta_id
INNER JOIN movement_values pb on pb.documento_soporte = CAST(op.orden_pago_id AS varchar) AND pb.fecha_creacion = tm.fecha_saldo_anterior AND tm.cuenta_id = pb.cuenta_id
),
payment_order_reversion_net_amount AS (
-- Transpone los valores de las reversiones de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND m.tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
)
SELECT TO_CHAR(result.date_operation_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
'UTC - 05:00' as time_zone,
result.account_id,
result.nombre AS merchant_account,
result.operation_type,
result.description,
result.reference ||'XXXXXXXXXXX' as reference,
result.transaction_id,
result.order_id,
'' AS batch_number_in_bank_deposit_file,
--result.payer_name,
--result.payer_document_number,
--result.payer_mail,
--result.payer_contact_phone,
--result.payer_address,
--result.payer_country,
result.currency_payment_request,
result.amount_payment_request,
result.payment_method,
--result.credit_card_number,
result.installments,
result.promotion,
result.authorization_code,
result.operation_currency,
result.operation_amount,
--result.reserved_amount,
result.payu_fee,
result.payu_fee_tax,
result.retentions,
result.months_without_interest_fee,
result.months_without_interest_tax,
result.interest,
result.interest_tax,
result.chargeback_fee,
'' AS chargeback_fee_tax,
result.net_amount,
'' AS exchange_rate,
'' AS remmited_currency,
'' AS operation_amount_remitted_currency,
'' AS payu_fee_remitted_currency,
'' AS payu_fee_tax_remitted_currency,
'' AS months_without_interest_fee_remitted_currency,
'' AS months_without_interest_tax_remitted_currency,
'' AS interest_remitted_currency,
'' AS interest_tax_remitted_currency,
'' AS chargeback_fee_remitted_currency,
'' AS chargeback_fee_tax_remitted_currency,
'' AS net_amount_remmited_currency,
--'' AS bank_wire_fee,
--'' net_amount_less_bank_wire_fee,
result.account_balance,
result.available_balance,
TO_CHAR(result.sales_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS sales_date
FROM (
-- Operation Type -> SELL, se genera a partir de transacciones de tipo CAPTURE, y AUTHORIZATION_AND_CAPTURE
SELECT 
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CAST('SELL' AS text) AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
t_payer_name AS payer_name,
t_payer_document_number AS payer_document_number,
t_payer_mail AS payer_mail,
t_payer_contact_phone AS payer_contact_phone,
t_payer_address AS payer_address,
t_payer_country AS payer_country,
COALESCE(ta.tx_moneda_iso_4217, '') AS currency_payment_request,
COALESCE(to_char(ta.tx_value, '999999999999999D99'), '') AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
gst.installments,
gst.promotion,
gst.authorization_code,
gst.operation_currency,
gst.operation_amount,
COALESCE(to_char(NULLIF((COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)), 0), '999999999999999D99'), '') AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_comision_payu, 0) + COALESCE(tm_tax_pol_commission, 0)), 0), '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_retention, 0) + COALESCE(tm_ica_retention, 0) + COALESCE(tm_renta_retention, 0) + COALESCE(tm_impuesto_renta_adicional, 0) + COALESCE(tm_iibb_merchant_commission, 0) + COALESCE(tm_iibb_payer_commission, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_tax_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_merchant_interest, 0) + COALESCE(tm_payer_interest, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_merchant_interest, 0) + COALESCE(tm_iibb_merchant_interest, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CAST('' AS text) AS chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)- (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
gst.sales_date
FROM generic_sales_table gst
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = gst.transaction_id 
WHERE t_tipo IN ('AUTHORIZATION_AND_CAPTURE', 'CAPTURE') 
AND pb_tipo_movimiento NOT IN ('REVERSION', 'CHARGEBACK', 'CHARGEBACK_FEE', 'FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> REVERSION, CHARGEBACK, PARTIAL_REFUND, se genera a partir de transacciones de tipo 'VOID', 'REFUND', 'CHARGEBACK' y 'PARTIAL_REFUND'
SELECT
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CASE WHEN t_tipo in ('VOID', 'REFUND') THEN CAST('REFUND' AS text) ELSE t_tipo END
AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
gst.t_payer_name,
t_payer_document_number,
t_payer_mail,
t_payer_contact_phone,
t_payer_address,
t_payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
'' AS "instalments",
gst.promotion,
gst.authorization_code,
gst.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_iva_comision_payu, 0) + COALESCE(tm_reversion_tax_comision_payu, 0)), 0), '999999999999999D99'), '') AS "payu_fee_tax",
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_reteiva, 0) + COALESCE(tm_reversion_reteica, 0) + COALESCE(tm_reversion_reterenta, 0) + COALESCE(tm_reversion_impuesto_renta_adicional, 0) + COALESCE(tm_reversion_comision_iibb_comercio, 0) + COALESCE(tm_reversion_comision_iibb_pagador, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_reversion_comision_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_reversion_tax_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_payer_reversion_intereses, 0) + COALESCE(tm_merchant_reversion_intereses, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_interes_iva_comercio, 0) + COALESCE(tm_reversion_interes_iibb_comercio, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CASE WHEN t_tipo = 'CHARGEBACK' THEN COALESCE(to_char(tm_chargeback_fee, '999999999999999D99'), '') ELSE '' END as chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM 
generic_sales_table gst
WHERE t_tipo IN ('VOID', 'REFUND', 'CHARGEBACK', 'PARTIAL_REFUND') 
AND pb_tipo_movimiento IN ('REVERSION', 'CHARGEBACK', 'PARTIAL_REFUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Type -> MONEY_TRANSFER, se genera a partir de transacciones de tipo 'CHARGEBACK'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER'
ELSE 'MONEY_TRANSFER' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER'
AND pb_tipo_movimiento = 'PAYMENT_ORDER'
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER_REFUND si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Tyoe -> MONEY_TRANSFER_REFUND, se genera a partir de transacciones de tipo 'PAYMENT_ORDER_REVERSION'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER_REFUND'
ELSE 'MONEY_TRANSFER_REFUND' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_reversion_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_reversion_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_reversion_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND pb_tipo_movimiento = 'PAYMENT_ORDER_REVERSION'
UNION
-- Operation Type -> PAYU_TRANSFER, se genera a partir de movimientos con tipo_documento_soporte = 'ACCOUNT_TRANSFER'
SELECT 
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id_origen,
tm.nombre,
'PAYU_TRANSFER' AS operation_type,
'' AS description,
'' AS reference,
tc.transferencia_cuenta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
'' AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_transfer_retention, 0)), 0), '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(NULLIF((COALESCE(tm.valor, 0)), 0), '999999999999999D99') AS net_amount,
to_char(tm.valor  + mv.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm.valor  + mv.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.transferencia_cuenta tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.transferencia_cuenta_id AND tm.cuenta_id = tc.cuenta_id_origen
INNER JOIN movement_values mv ON mv.documento_soporte = tc.transferencia_cuenta_id AND mv.tipo_movimiento = 'ACCOUNT_TRANSFER' AND mv.fecha_creacion = tm.fecha_saldo_anterior
WHERE tm_tipo_documento_soporte = 'ACCOUNT_TRANSFER'
UNION
-- Operation Type -> DISCRETIONAL_MOVEMENT, se genera a partir de movimientos con tipo_documento_soporte = 'DISCRETIONARY'
SELECT 
tm.fecha_creacion date_operation_date,
CAST(md.cuenta_id AS int) AS account_id,
tm.nombre,
'DISCRETIONAL_MOVEMENT' AS operation_type,
md.descripcion AS description,
'' AS reference,
md.movimiento_discrecional_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.movimiento_discrecional md
INNER JOIN transposed_movements tm ON tm.documento_soporte = md.movimiento_discrecional_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = md.movimiento_discrecional_id AND pb.tipo_movimiento IN ('DISCRETIONARY') 
WHERE tm.tm_tipo_documento_soporte = 'DISCRETIONARY'
UNION
-- Operation Type -> PAYMENT_CARDS, se genera a partir de movimientos con tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
SELECT
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id AS account_id,
tm.nombre,
'PAYMENT_CARDS' AS operation_type,
'' AS description,
'' AS reference,
tc.solicitud_tarjeta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  -(COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0) ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.solicitud_tarjeta_cobranza tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.solicitud_tarjeta_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = tc.solicitud_tarjeta_id AND pb.tipo_movimiento IN ('PAYMENT_CARD_REQUEST') 
WHERE tm.tm_tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
UNION
-- Operation type -> FREEZE si tipo_movimiento = 'FREEZE_FUND'
-- Operation type -> RELEASE RESERVE si tipo_movimiento = 'RELEASE_RESERVE_FUND'
-- Operation type -> UNFREEZE si no se cumplen las anteriores
SELECT
mv.fecha_creacion AS date_operation_date,
mv.cuenta_id,
mv.nombre,
CASE 
WHEN tipo_movimiento = 'FREEZE_FUND' THEN 'FREEZE' 
WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN 'RELEASE RESERVE'
ELSE 'UNFREEZE'
END AS operation_type,
'' AS description,
'' AS reference,
transaccion_id,
CAST(orden_id AS varchar),
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(mv.valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(to_char(NULLIF((CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE 0 END ), 0), '999999999999999D99'), '') AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
'' AS net_amount,
to_char(saldo_anterior, '999999999999999D99') AS account_balance,
to_char(saldo_anterior + COALESCE(mv.valor, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)), '999999999999999D99') AS available_balance,
t.fecha_creacion AS sales_date
FROM 
transactions t
INNER JOIN movement_values mv ON t.transaccion_id = mv.documento_soporte AND tipo_movimiento IN ('FREEZE_FUND', 'UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
) as result order by date_operation_date, account_balance;
""",{'first_date':first_date,'last_date':last_date})
    monthlyreport=pd.DataFrame(cursor.fetchall())
    return monthlyreport

# Reportes Para Cuenta de Colombia 642552
def createmonthlyreportforcolombiaacc642552(first_date,last_date):
    #%(first_date)s
    cursor.execute("""WITH 
movement_values AS (
-- Selecciona todos los movimientos, modifica el valor ( * -1) según el tipo de operacion (NC - Crédito, ND - Débito)
SELECT 
m.cuenta_id,
c.nombre,
c.moneda_iso_4217,
m.documento_soporte, 
m.tipo_documento_soporte, 
m.tipo_movimiento, 
m.descripcion, 
m.saldo_anterior, 
saldo_congelado_anterior,
saldo_reserva_anterior,
m.fecha_creacion,
CASE
WHEN transaccion_relacionada_id IS NULL 
THEN documento_soporte
ELSE transaccion_relacionada_id END AS transaccion_relacionada_id,
CASE 
WHEN m.operacion = 'NC' THEN m.valor
ELSE m.valor * -1 
END AS valor
FROM pps.movimiento m 
INNER JOIN pps.cuenta c ON c.cuenta_id = m.cuenta_id
WHERE m.fecha_creacion BETWEEN %(first_date)s AND %(last_date)s AND m.cuenta_id = 642552
),
transposed_movements AS (
-- Transpone los valores de los movimientos agrupandolos según el tipo de movimiento
SELECT
cuenta_id, nombre, documento_soporte, transaccion_relacionada_id, tipo_documento_soporte AS tm_tipo_documento_soporte,
MIN(fecha_creacion) fecha_saldo_anterior,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN fecha_creacion ELSE NULL END) AS fecha_creacion,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN valor ELSE NULL END) AS tm_valor,
MAX(CASE WHEN tipo_movimiento IN ('POL_COMMISSION', 'PAYMENT_ORDER_POL_COMMISION', 'PAYMENT_ORDER_POL_COMMISION_SUPPLIER', 'ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE NULL END) AS tm_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('IVA_POL_COMMISSION', 'IVA_PAYMENT_ORDER_POL_COMMISION', 'IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER','IVA_ACCOUNT_TRANSFER_COMMISSION') THEN valor ELSE NULL END) AS tm_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento = 'ADDITIONAL_WITHHOLDING_TAX' THEN valor ELSE NULL END) AS tm_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento = 'ICA_RETENTION' THEN valor ELSE NULL END) AS tm_ica_retention,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_merchant_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iibb_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYER_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iva_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IVA_PAYER_COMMISSION' THEN valor ELSE NULL END) AS tm_iva_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_RETENTION' THEN valor ELSE NULL END) AS tm_iva_retention,
MAX(CASE WHEN tipo_movimiento = 'MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'PAYER_INTEREST' THEN valor ELSE NULL END) AS tm_payer_interest,
MAX(CASE WHEN tipo_movimiento = 'MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE NULL END) AS tm_release_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'RENTA_RETENTION' THEN valor ELSE NULL END) AS tm_renta_retention,
MAX(CASE WHEN tipo_movimiento = 'RESERVE_FUND' THEN valor ELSE NULL END) AS tm_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'TAX_MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_tax_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'TAX_POL_COMMISSION' THEN valor ELSE NULL END) AS tm_tax_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYMENT_ORDER_POL_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payment_order_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'PAYMENT_ORDER_REVERSION' AND descripcion ILIKE concat(tipo_movimiento,'%%IIBB_PAYMENT_ORDER_POL_COMMISION%%') THEN valor ELSE NULL END) AS tm_iibb_payment_order_reversion_pol_commission,
SUM(CASE WHEN tipo_movimiento in ('TAX_LAW_25413_DEBIT', 'TAX_LAW_25413_CREDIT', 'IIBB_ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE 0 END) AS tm_transfer_retention,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN fecha_creacion ELSE NULL END) AS tm_reversion_fecha,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN valor ELSE NULL END) AS tm_reversion_valor,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_POL_COMMISSION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteiva,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ICA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteica,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' RENTA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reterenta,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ADDITIONAL_WITHHOLDING_TAX%%') THEN valor ELSE NULL END) AS tm_reversion_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_PAYER_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_pagador,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iva_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_merchant_reversion_intereses,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' PAYER_INTEREST%%') THEN valor ELSE NULL END) AS tm_payer_reversion_intereses,
MAX(CASE WHEN tipo_movimiento = 'CHARGEBACK_FEE' THEN valor ELSE NULL END) AS tm_chargeback_fee,
SUM(CASE WHEN tipo_movimiento NOT IN('FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND', 'RESERVE_FUND')  THEN valor ELSE NULL END) as valor
FROM 
movement_values
GROUP BY 
1, 2, 3, 4, 5
),
movement_prev_balance AS (
-- Obtiene el balance previo tomado a partir de la fecha obtenida en la consulta transposed_movements.
SELECT m.documento_soporte, m.transaccion_relacionada_id, tm_tipo_documento_soporte, m.tipo_movimiento, saldo_anterior,saldo_congelado_anterior,saldo_reserva_anterior,moneda_iso_4217 
FROM transposed_movements t 
INNER JOIN  movement_values m on t.documento_soporte = m.documento_soporte AND t.fecha_saldo_anterior = m.fecha_creacion and t.cuenta_id = m.cuenta_id
),
transactions AS (               
--Obtiene las transacciones que tienen movimiento_soporte tipo ORDEN y tipo aut&capt y  captura
SELECT                      
t.orden_id,
t.transaccion_id,
t.convenio_id,
t.tipo,
t.transaccion_padre_id,
t.codigo_autorizacion,
t.fecha_creacion,
t.pagador_email,
t.pagador_nombre_completo,
t.pagador_numero_identificacion,
t.pagador_telefono_contacto,
t.pagador_direccion_cobro_pais,
t.pagador_direccion_cobro_calle1,
t.tarjeta_credito_id
FROM pps.transaccion t 
INNER JOIN 
(
SELECT DISTINCT transaccion_relacionada_id 
FROM movement_values 
WHERE tipo_documento_soporte in ('ORDER', 'REVERSION')
) mv ON t.transaccion_id = mv.transaccion_relacionada_id),
generic_sales_table AS (
-- Cruza los movientos de ventas con las tablas de transacciones para obtener los valores originales
SELECT 
tm.*,
pb.saldo_reserva_anterior,
pb.saldo_congelado_anterior,
TO_CHAR(tm.fecha_creacion, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
tm.fecha_creacion AS tm_fecha_creacion,
t.tipo AS t_tipo,
t.transaccion_padre_id,
t.transaccion_id AS transaction_id,
t.fecha_creacion AS sales_date,
o.cuenta_id AS account_id,
o.referencia AS reference,
o.descripcion AS description,
o.comprador_email,
o.direccion_envio_pais, 
o.direccion_envio_calle1, 
o.comprador_direccion_pais, 
o.direccion_envio_telefono, 
o.comprador_nombre_completo, 
o.comprador_direccion_calle1,
o.comprador_telefono_contacto, 
o.comprador_direccion_telefono,
o.comprador_numero_identificacion,
c.tipo_medio_pago AS payment_method,
tc.nombre AS tc_nombre,
pb.saldo_anterior AS pb_saldo_anterior,
pb.tipo_movimiento AS pb_tipo_movimiento,
CAST(o.orden_id AS varchar) AS order_id,
COALESCE(pm.nombre, '') AS promotion,
COALESCE(tc.numero_visible, '') AS credit_card_number,
COALESCE(tpe.installments_number, '') AS installments,
COALESCE(t.codigo_autorizacion, '') AS authorization_code,
COALESCE(pb.moneda_iso_4217, '') AS operation_currency,
COALESCE(t.pagador_email, o.comprador_email, '') AS t_payer_mail,
COALESCE(to_char(tm.tm_valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(t.pagador_nombre_completo, o.comprador_nombre_completo, tc.nombre, '') AS t_payer_name,
COALESCE(t.pagador_numero_identificacion, o.comprador_numero_identificacion, '') AS t_payer_document_number,
REPLACE(COALESCE(o.direccion_envio_pais, o.comprador_direccion_pais, t.pagador_direccion_cobro_pais, ''), ';', ',') AS t_payer_country,
REPLACE(COALESCE(o.direccion_envio_calle1, o.comprador_direccion_calle1, t.pagador_direccion_cobro_calle1, ''), ';', ',') AS t_payer_address,
REPLACE(COALESCE(t.pagador_telefono_contacto, o.comprador_telefono_contacto, o.direccion_envio_telefono, o.comprador_direccion_telefono, ''), ';', ',') AS t_payer_contact_phone
FROM transactions t
INNER JOIN transposed_movements tm ON tm.transaccion_relacionada_id = t.transaccion_id
INNER JOIN movement_prev_balance pb on pb.transaccion_relacionada_id = t.transaccion_id 
INNER JOIN pps.orden o ON o.orden_id = t.orden_id
INNER JOIN pps.convenio c ON c.convenio_id = t.convenio_id
LEFT JOIN pps.tarjeta_credito tc ON t.tarjeta_credito_id = tc.tarjeta_credito_id
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = t.transaccion_id 
LEFT JOIN pps.transaccion_datos_extra tpe ON tpe.transaccion_id = t.transaccion_id 
LEFT JOIN pps.promocion pm ON pm.promocion_id = CAST(tpe.promotion_id AS INT)
),
payment_orders AS (
-- Cruza los movimientos con las ordenes de pago para obtener los valores originales
SELECT op.* FROM pps.orden_pago op
INNER JOIN movement_values m ON m.documento_soporte = CAST(op.orden_pago_id AS varchar) AND m.tipo_documento_soporte = 'PAYMENT_ORDER'
WHERE op.cuenta_id = m.cuenta_id
),
payment_order_net_amount AS (
-- Transpone los valores de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER'
AND m.tipo_movimiento NOT IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
),
payments_order_table AS (
-- Obtiene los valores originales de la orden de pago
SELECT 
tm.fecha_creacion AS operation_date,
CAST(op.cuenta_id AS int) AS account_id,
tm.nombre,
op.tipo_transferencia_orden_pago,
op.orden_pago_id,
pb.tipo_movimiento AS pb_tipo_movimiento,
COALESCE(moneda_iso_4217, '') AS operation_currency,
tm_valor,
tm_comision_payu,
tm_iva_comision_payu,
tm_reversion_valor,
tm_reversion_comision_payu,
tm_reversion_iva_comision_payu,
tm_tipo_documento_soporte,
tm_iibb_payment_order_pol_commission,
tm_iibb_payment_order_reversion_pol_commission,
pb.saldo_anterior AS pb_saldo_anterior,
saldo_congelado_anterior,
saldo_reserva_anterior
FROM
payment_orders op
INNER JOIN transposed_movements tm ON tm.documento_soporte = CAST(op.orden_pago_id AS varchar) AND tm.cuenta_id = op.cuenta_id
INNER JOIN movement_values pb on pb.documento_soporte = CAST(op.orden_pago_id AS varchar) AND pb.fecha_creacion = tm.fecha_saldo_anterior AND tm.cuenta_id = pb.cuenta_id
),
payment_order_reversion_net_amount AS (
-- Transpone los valores de las reversiones de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND m.tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
)
SELECT TO_CHAR(result.date_operation_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
'UTC - 05:00' as time_zone,
result.account_id,
result.nombre AS merchant_account,
result.operation_type,
result.description,
result.reference ||'XXXXXXXXXXX' as reference,
result.transaction_id,
result.order_id,
'' AS batch_number_in_bank_deposit_file,
--result.payer_name,
--result.payer_document_number,
--result.payer_mail,
--result.payer_contact_phone,
--result.payer_address,
--result.payer_country,
result.currency_payment_request,
result.amount_payment_request,
result.payment_method,
--result.credit_card_number,
result.installments,
result.promotion,
result.authorization_code,
result.operation_currency,
result.operation_amount,
--result.reserved_amount,
result.payu_fee,
result.payu_fee_tax,
result.retentions,
result.months_without_interest_fee,
result.months_without_interest_tax,
result.interest,
result.interest_tax,
result.chargeback_fee,
'' AS chargeback_fee_tax,
result.net_amount,
'' AS exchange_rate,
'' AS remmited_currency,
'' AS operation_amount_remitted_currency,
'' AS payu_fee_remitted_currency,
'' AS payu_fee_tax_remitted_currency,
'' AS months_without_interest_fee_remitted_currency,
'' AS months_without_interest_tax_remitted_currency,
'' AS interest_remitted_currency,
'' AS interest_tax_remitted_currency,
'' AS chargeback_fee_remitted_currency,
'' AS chargeback_fee_tax_remitted_currency,
'' AS net_amount_remmited_currency,
--'' AS bank_wire_fee,
--'' net_amount_less_bank_wire_fee,
result.account_balance,
result.available_balance,
TO_CHAR(result.sales_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS sales_date
FROM (
-- Operation Type -> SELL, se genera a partir de transacciones de tipo CAPTURE, y AUTHORIZATION_AND_CAPTURE
SELECT 
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CAST('SELL' AS text) AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
t_payer_name AS payer_name,
t_payer_document_number AS payer_document_number,
t_payer_mail AS payer_mail,
t_payer_contact_phone AS payer_contact_phone,
t_payer_address AS payer_address,
t_payer_country AS payer_country,
COALESCE(ta.tx_moneda_iso_4217, '') AS currency_payment_request,
COALESCE(to_char(ta.tx_value, '999999999999999D99'), '') AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
gst.installments,
gst.promotion,
gst.authorization_code,
gst.operation_currency,
gst.operation_amount,
COALESCE(to_char(NULLIF((COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)), 0), '999999999999999D99'), '') AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_comision_payu, 0) + COALESCE(tm_tax_pol_commission, 0)), 0), '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_retention, 0) + COALESCE(tm_ica_retention, 0) + COALESCE(tm_renta_retention, 0) + COALESCE(tm_impuesto_renta_adicional, 0) + COALESCE(tm_iibb_merchant_commission, 0) + COALESCE(tm_iibb_payer_commission, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_tax_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_merchant_interest, 0) + COALESCE(tm_payer_interest, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_merchant_interest, 0) + COALESCE(tm_iibb_merchant_interest, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CAST('' AS text) AS chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)- (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
gst.sales_date
FROM generic_sales_table gst
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = gst.transaction_id 
WHERE t_tipo IN ('AUTHORIZATION_AND_CAPTURE', 'CAPTURE') 
AND pb_tipo_movimiento NOT IN ('REVERSION', 'CHARGEBACK', 'CHARGEBACK_FEE', 'FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> REVERSION, CHARGEBACK, PARTIAL_REFUND, se genera a partir de transacciones de tipo 'VOID', 'REFUND', 'CHARGEBACK' y 'PARTIAL_REFUND'
SELECT
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CASE WHEN t_tipo in ('VOID', 'REFUND') THEN CAST('REFUND' AS text) ELSE t_tipo END
AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
gst.t_payer_name,
t_payer_document_number,
t_payer_mail,
t_payer_contact_phone,
t_payer_address,
t_payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
'' AS "instalments",
gst.promotion,
gst.authorization_code,
gst.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_iva_comision_payu, 0) + COALESCE(tm_reversion_tax_comision_payu, 0)), 0), '999999999999999D99'), '') AS "payu_fee_tax",
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_reteiva, 0) + COALESCE(tm_reversion_reteica, 0) + COALESCE(tm_reversion_reterenta, 0) + COALESCE(tm_reversion_impuesto_renta_adicional, 0) + COALESCE(tm_reversion_comision_iibb_comercio, 0) + COALESCE(tm_reversion_comision_iibb_pagador, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_reversion_comision_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_reversion_tax_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_payer_reversion_intereses, 0) + COALESCE(tm_merchant_reversion_intereses, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_interes_iva_comercio, 0) + COALESCE(tm_reversion_interes_iibb_comercio, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CASE WHEN t_tipo = 'CHARGEBACK' THEN COALESCE(to_char(tm_chargeback_fee, '999999999999999D99'), '') ELSE '' END as chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM 
generic_sales_table gst
WHERE t_tipo IN ('VOID', 'REFUND', 'CHARGEBACK', 'PARTIAL_REFUND') 
AND pb_tipo_movimiento IN ('REVERSION', 'CHARGEBACK', 'PARTIAL_REFUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Type -> MONEY_TRANSFER, se genera a partir de transacciones de tipo 'CHARGEBACK'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER'
ELSE 'MONEY_TRANSFER' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER'
AND pb_tipo_movimiento = 'PAYMENT_ORDER'
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER_REFUND si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Tyoe -> MONEY_TRANSFER_REFUND, se genera a partir de transacciones de tipo 'PAYMENT_ORDER_REVERSION'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER_REFUND'
ELSE 'MONEY_TRANSFER_REFUND' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_reversion_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_reversion_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_reversion_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND pb_tipo_movimiento = 'PAYMENT_ORDER_REVERSION'
UNION
-- Operation Type -> PAYU_TRANSFER, se genera a partir de movimientos con tipo_documento_soporte = 'ACCOUNT_TRANSFER'
SELECT 
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id_origen,
tm.nombre,
'PAYU_TRANSFER' AS operation_type,
'' AS description,
'' AS reference,
tc.transferencia_cuenta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
'' AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_transfer_retention, 0)), 0), '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(NULLIF((COALESCE(tm.valor, 0)), 0), '999999999999999D99') AS net_amount,
to_char(tm.valor  + mv.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm.valor  + mv.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.transferencia_cuenta tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.transferencia_cuenta_id AND tm.cuenta_id = tc.cuenta_id_origen
INNER JOIN movement_values mv ON mv.documento_soporte = tc.transferencia_cuenta_id AND mv.tipo_movimiento = 'ACCOUNT_TRANSFER' AND mv.fecha_creacion = tm.fecha_saldo_anterior
WHERE tm_tipo_documento_soporte = 'ACCOUNT_TRANSFER'
UNION
-- Operation Type -> DISCRETIONAL_MOVEMENT, se genera a partir de movimientos con tipo_documento_soporte = 'DISCRETIONARY'
SELECT 
tm.fecha_creacion date_operation_date,
CAST(md.cuenta_id AS int) AS account_id,
tm.nombre,
'DISCRETIONAL_MOVEMENT' AS operation_type,
md.descripcion AS description,
'' AS reference,
md.movimiento_discrecional_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.movimiento_discrecional md
INNER JOIN transposed_movements tm ON tm.documento_soporte = md.movimiento_discrecional_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = md.movimiento_discrecional_id AND pb.tipo_movimiento IN ('DISCRETIONARY') 
WHERE tm.tm_tipo_documento_soporte = 'DISCRETIONARY'
UNION
-- Operation Type -> PAYMENT_CARDS, se genera a partir de movimientos con tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
SELECT
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id AS account_id,
tm.nombre,
'PAYMENT_CARDS' AS operation_type,
'' AS description,
'' AS reference,
tc.solicitud_tarjeta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  -(COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0) ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.solicitud_tarjeta_cobranza tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.solicitud_tarjeta_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = tc.solicitud_tarjeta_id AND pb.tipo_movimiento IN ('PAYMENT_CARD_REQUEST') 
WHERE tm.tm_tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
UNION
-- Operation type -> FREEZE si tipo_movimiento = 'FREEZE_FUND'
-- Operation type -> RELEASE RESERVE si tipo_movimiento = 'RELEASE_RESERVE_FUND'
-- Operation type -> UNFREEZE si no se cumplen las anteriores
SELECT
mv.fecha_creacion AS date_operation_date,
mv.cuenta_id,
mv.nombre,
CASE 
WHEN tipo_movimiento = 'FREEZE_FUND' THEN 'FREEZE' 
WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN 'RELEASE RESERVE'
ELSE 'UNFREEZE'
END AS operation_type,
'' AS description,
'' AS reference,
transaccion_id,
CAST(orden_id AS varchar),
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(mv.valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(to_char(NULLIF((CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE 0 END ), 0), '999999999999999D99'), '') AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
'' AS net_amount,
to_char(saldo_anterior, '999999999999999D99') AS account_balance,
to_char(saldo_anterior + COALESCE(mv.valor, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)), '999999999999999D99') AS available_balance,
t.fecha_creacion AS sales_date
FROM 
transactions t
INNER JOIN movement_values mv ON t.transaccion_id = mv.documento_soporte AND tipo_movimiento IN ('FREEZE_FUND', 'UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
) as result order by date_operation_date, account_balance;
""",{'first_date':first_date,'last_date':last_date})
    monthlyreport=pd.DataFrame(cursor.fetchall())
    return monthlyreport

# Reportes Para Cuenta de Colombia 642519

def createmonthlyreportforcolombiaacc642519(first_date,last_date):
    #%(first_date)s
    cursor.execute("""WITH 
movement_values AS (
-- Selecciona todos los movimientos, modifica el valor ( * -1) según el tipo de operacion (NC - Crédito, ND - Débito)
SELECT 
m.cuenta_id,
c.nombre,
c.moneda_iso_4217,
m.documento_soporte, 
m.tipo_documento_soporte, 
m.tipo_movimiento, 
m.descripcion, 
m.saldo_anterior, 
saldo_congelado_anterior,
saldo_reserva_anterior,
m.fecha_creacion,
CASE
WHEN transaccion_relacionada_id IS NULL 
THEN documento_soporte
ELSE transaccion_relacionada_id END AS transaccion_relacionada_id,
CASE 
WHEN m.operacion = 'NC' THEN m.valor
ELSE m.valor * -1 
END AS valor
FROM pps.movimiento m 
INNER JOIN pps.cuenta c ON c.cuenta_id = m.cuenta_id
WHERE m.fecha_creacion BETWEEN %(first_date)s AND %(last_date)s AND m.cuenta_id = 642519
),
transposed_movements AS (
-- Transpone los valores de los movimientos agrupandolos según el tipo de movimiento
SELECT
cuenta_id, nombre, documento_soporte, transaccion_relacionada_id, tipo_documento_soporte AS tm_tipo_documento_soporte,
MIN(fecha_creacion) fecha_saldo_anterior,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN fecha_creacion ELSE NULL END) AS fecha_creacion,
MAX(CASE WHEN tipo_movimiento IN ('SALES', 'PAYMENT_ORDER', 'PAYMENT_ORDER_SUPPLIER', 'ACCOUNT_TRANSFER', 'DISCRETIONARY', 'PAYMENT_CARD_REQUEST') THEN valor ELSE NULL END) AS tm_valor,
MAX(CASE WHEN tipo_movimiento IN ('POL_COMMISSION', 'PAYMENT_ORDER_POL_COMMISION', 'PAYMENT_ORDER_POL_COMMISION_SUPPLIER', 'ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE NULL END) AS tm_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('IVA_POL_COMMISSION', 'IVA_PAYMENT_ORDER_POL_COMMISION', 'IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER','IVA_ACCOUNT_TRANSFER_COMMISSION') THEN valor ELSE NULL END) AS tm_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento = 'ADDITIONAL_WITHHOLDING_TAX' THEN valor ELSE NULL END) AS tm_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento = 'ICA_RETENTION' THEN valor ELSE NULL END) AS tm_ica_retention,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_merchant_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iibb_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYER_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_iva_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'IVA_PAYER_COMMISSION' THEN valor ELSE NULL END) AS tm_iva_payer_commission,
MAX(CASE WHEN tipo_movimiento = 'IVA_RETENTION' THEN valor ELSE NULL END) AS tm_iva_retention,
MAX(CASE WHEN tipo_movimiento = 'MERCHANT_INTEREST' THEN valor ELSE NULL END) AS tm_merchant_interest,
MAX(CASE WHEN tipo_movimiento = 'PAYER_INTEREST' THEN valor ELSE NULL END) AS tm_payer_interest,
MAX(CASE WHEN tipo_movimiento = 'MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE NULL END) AS tm_release_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'RENTA_RETENTION' THEN valor ELSE NULL END) AS tm_renta_retention,
MAX(CASE WHEN tipo_movimiento = 'RESERVE_FUND' THEN valor ELSE NULL END) AS tm_reserve_fund,
MAX(CASE WHEN tipo_movimiento = 'TAX_MONTHS_WITHOUT_INTEREST_COMMISION' THEN valor ELSE NULL END) AS tm_tax_months_without_interest_commission,
MAX(CASE WHEN tipo_movimiento = 'TAX_POL_COMMISSION' THEN valor ELSE NULL END) AS tm_tax_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'IIBB_PAYMENT_ORDER_POL_COMMISION' THEN valor ELSE NULL END) AS tm_iibb_payment_order_pol_commission,
MAX(CASE WHEN tipo_movimiento = 'PAYMENT_ORDER_REVERSION' AND descripcion ILIKE concat(tipo_movimiento,'%%IIBB_PAYMENT_ORDER_POL_COMMISION%%') THEN valor ELSE NULL END) AS tm_iibb_payment_order_reversion_pol_commission,
SUM(CASE WHEN tipo_movimiento in ('TAX_LAW_25413_DEBIT', 'TAX_LAW_25413_CREDIT', 'IIBB_ACCOUNT_TRANSFER_COMISSION') THEN valor ELSE 0 END) AS tm_transfer_retention,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN fecha_creacion ELSE NULL END) AS tm_reversion_fecha,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' SALES%%') OR tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') THEN valor ELSE NULL END) AS tm_reversion_valor,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_POL_COMMISSION%%') OR (tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION') AND (descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION%%' OR descripcion ILIKE '%%IVA_PAYMENT_ORDER_POL_COMMISION_SUPPLIER%%')) THEN valor ELSE NULL END) AS tm_reversion_iva_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_POL_COMMISSION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_comision_payu,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteiva,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ICA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reteica,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' RENTA_RETENTION%%') THEN valor ELSE NULL END) AS tm_reversion_reterenta,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' ADDITIONAL_WITHHOLDING_TAX%%') THEN valor ELSE NULL END) AS tm_reversion_impuesto_renta_adicional,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_PAYER_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_iibb_pagador,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IVA_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iva_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' IIBB_MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_reversion_interes_iibb_comercio,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_comision_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' TAX_MONTHS_WITHOUT_INTEREST_COMMISION%%') THEN valor ELSE NULL END) AS tm_reversion_tax_interes_sin_mes,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' MERCHANT_INTEREST%%') THEN valor ELSE NULL END) AS tm_merchant_reversion_intereses,
MAX(CASE WHEN tipo_movimiento IN ('REVERSION','PARTIAL_REFUND','CHARGEBACK') AND descripcion ILIKE concat(tipo_movimiento,' PAYER_INTEREST%%') THEN valor ELSE NULL END) AS tm_payer_reversion_intereses,
MAX(CASE WHEN tipo_movimiento = 'CHARGEBACK_FEE' THEN valor ELSE NULL END) AS tm_chargeback_fee,
SUM(CASE WHEN tipo_movimiento NOT IN('FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND', 'RESERVE_FUND')  THEN valor ELSE NULL END) as valor
FROM 
movement_values
GROUP BY 
1, 2, 3, 4, 5
),
movement_prev_balance AS (
-- Obtiene el balance previo tomado a partir de la fecha obtenida en la consulta transposed_movements.
SELECT m.documento_soporte, m.transaccion_relacionada_id, tm_tipo_documento_soporte, m.tipo_movimiento, saldo_anterior,saldo_congelado_anterior,saldo_reserva_anterior,moneda_iso_4217 
FROM transposed_movements t 
INNER JOIN  movement_values m on t.documento_soporte = m.documento_soporte AND t.fecha_saldo_anterior = m.fecha_creacion and t.cuenta_id = m.cuenta_id
),
transactions AS (               
--Obtiene las transacciones que tienen movimiento_soporte tipo ORDEN y tipo aut&capt y  captura
SELECT                      
t.orden_id,
t.transaccion_id,
t.convenio_id,
t.tipo,
t.transaccion_padre_id,
t.codigo_autorizacion,
t.fecha_creacion,
t.pagador_email,
t.pagador_nombre_completo,
t.pagador_numero_identificacion,
t.pagador_telefono_contacto,
t.pagador_direccion_cobro_pais,
t.pagador_direccion_cobro_calle1,
t.tarjeta_credito_id
FROM pps.transaccion t 
INNER JOIN 
(
SELECT DISTINCT transaccion_relacionada_id 
FROM movement_values 
WHERE tipo_documento_soporte in ('ORDER', 'REVERSION')
) mv ON t.transaccion_id = mv.transaccion_relacionada_id),
generic_sales_table AS (
-- Cruza los movientos de ventas con las tablas de transacciones para obtener los valores originales
SELECT 
tm.*,
pb.saldo_reserva_anterior,
pb.saldo_congelado_anterior,
TO_CHAR(tm.fecha_creacion, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
tm.fecha_creacion AS tm_fecha_creacion,
t.tipo AS t_tipo,
t.transaccion_padre_id,
t.transaccion_id AS transaction_id,
t.fecha_creacion AS sales_date,
o.cuenta_id AS account_id,
o.referencia AS reference,
o.descripcion AS description,
o.comprador_email,
o.direccion_envio_pais, 
o.direccion_envio_calle1, 
o.comprador_direccion_pais, 
o.direccion_envio_telefono, 
o.comprador_nombre_completo, 
o.comprador_direccion_calle1,
o.comprador_telefono_contacto, 
o.comprador_direccion_telefono,
o.comprador_numero_identificacion,
c.tipo_medio_pago AS payment_method,
tc.nombre AS tc_nombre,
pb.saldo_anterior AS pb_saldo_anterior,
pb.tipo_movimiento AS pb_tipo_movimiento,
CAST(o.orden_id AS varchar) AS order_id,
COALESCE(pm.nombre, '') AS promotion,
COALESCE(tc.numero_visible, '') AS credit_card_number,
COALESCE(tpe.installments_number, '') AS installments,
COALESCE(t.codigo_autorizacion, '') AS authorization_code,
COALESCE(pb.moneda_iso_4217, '') AS operation_currency,
COALESCE(t.pagador_email, o.comprador_email, '') AS t_payer_mail,
COALESCE(to_char(tm.tm_valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(t.pagador_nombre_completo, o.comprador_nombre_completo, tc.nombre, '') AS t_payer_name,
COALESCE(t.pagador_numero_identificacion, o.comprador_numero_identificacion, '') AS t_payer_document_number,
REPLACE(COALESCE(o.direccion_envio_pais, o.comprador_direccion_pais, t.pagador_direccion_cobro_pais, ''), ';', ',') AS t_payer_country,
REPLACE(COALESCE(o.direccion_envio_calle1, o.comprador_direccion_calle1, t.pagador_direccion_cobro_calle1, ''), ';', ',') AS t_payer_address,
REPLACE(COALESCE(t.pagador_telefono_contacto, o.comprador_telefono_contacto, o.direccion_envio_telefono, o.comprador_direccion_telefono, ''), ';', ',') AS t_payer_contact_phone
FROM transactions t
INNER JOIN transposed_movements tm ON tm.transaccion_relacionada_id = t.transaccion_id
INNER JOIN movement_prev_balance pb on pb.transaccion_relacionada_id = t.transaccion_id 
INNER JOIN pps.orden o ON o.orden_id = t.orden_id
INNER JOIN pps.convenio c ON c.convenio_id = t.convenio_id
LEFT JOIN pps.tarjeta_credito tc ON t.tarjeta_credito_id = tc.tarjeta_credito_id
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = t.transaccion_id 
LEFT JOIN pps.transaccion_datos_extra tpe ON tpe.transaccion_id = t.transaccion_id 
LEFT JOIN pps.promocion pm ON pm.promocion_id = CAST(tpe.promotion_id AS INT)
),
payment_orders AS (
-- Cruza los movimientos con las ordenes de pago para obtener los valores originales
SELECT op.* FROM pps.orden_pago op
INNER JOIN movement_values m ON m.documento_soporte = CAST(op.orden_pago_id AS varchar) AND m.tipo_documento_soporte = 'PAYMENT_ORDER'
WHERE op.cuenta_id = m.cuenta_id
),
payment_order_net_amount AS (
-- Transpone los valores de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER'
AND m.tipo_movimiento NOT IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
),
payments_order_table AS (
-- Obtiene los valores originales de la orden de pago
SELECT 
tm.fecha_creacion AS operation_date,
CAST(op.cuenta_id AS int) AS account_id,
tm.nombre,
op.tipo_transferencia_orden_pago,
op.orden_pago_id,
pb.tipo_movimiento AS pb_tipo_movimiento,
COALESCE(moneda_iso_4217, '') AS operation_currency,
tm_valor,
tm_comision_payu,
tm_iva_comision_payu,
tm_reversion_valor,
tm_reversion_comision_payu,
tm_reversion_iva_comision_payu,
tm_tipo_documento_soporte,
tm_iibb_payment_order_pol_commission,
tm_iibb_payment_order_reversion_pol_commission,
pb.saldo_anterior AS pb_saldo_anterior,
saldo_congelado_anterior,
saldo_reserva_anterior
FROM
payment_orders op
INNER JOIN transposed_movements tm ON tm.documento_soporte = CAST(op.orden_pago_id AS varchar) AND tm.cuenta_id = op.cuenta_id
INNER JOIN movement_values pb on pb.documento_soporte = CAST(op.orden_pago_id AS varchar) AND pb.fecha_creacion = tm.fecha_saldo_anterior AND tm.cuenta_id = pb.cuenta_id
),
payment_order_reversion_net_amount AS (
-- Transpone los valores de las reversiones de las ordenes de pago para obtener los valores agregados
SELECT CAST(m.documento_soporte AS bigint) AS documento_soporte, SUM(m.valor) AS valor
FROM movement_values m 
WHERE m.tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND m.tipo_movimiento IN ('PAYMENT_ORDER_REVERSION', 'PAYMENT_ORDER_SUPPLIER_REVERSION')
GROUP BY documento_soporte
)
SELECT TO_CHAR(result.date_operation_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS operation_date,
'UTC - 05:00' as time_zone,
result.account_id,
result.nombre AS merchant_account,
result.operation_type,
result.description,
result.reference ||'XXXXXXXXXXX' as reference,
result.transaction_id,
result.order_id,
'' AS batch_number_in_bank_deposit_file,
--result.payer_name,
--result.payer_document_number,
--result.payer_mail,
--result.payer_contact_phone,
--result.payer_address,
--result.payer_country,
result.currency_payment_request,
result.amount_payment_request,
result.payment_method,
--result.credit_card_number,
result.installments,
result.promotion,
result.authorization_code,
result.operation_currency,
result.operation_amount,
--result.reserved_amount,
result.payu_fee,
result.payu_fee_tax,
result.retentions,
result.months_without_interest_fee,
result.months_without_interest_tax,
result.interest,
result.interest_tax,
result.chargeback_fee,
'' AS chargeback_fee_tax,
result.net_amount,
'' AS exchange_rate,
'' AS remmited_currency,
'' AS operation_amount_remitted_currency,
'' AS payu_fee_remitted_currency,
'' AS payu_fee_tax_remitted_currency,
'' AS months_without_interest_fee_remitted_currency,
'' AS months_without_interest_tax_remitted_currency,
'' AS interest_remitted_currency,
'' AS interest_tax_remitted_currency,
'' AS chargeback_fee_remitted_currency,
'' AS chargeback_fee_tax_remitted_currency,
'' AS net_amount_remmited_currency,
--'' AS bank_wire_fee,
--'' net_amount_less_bank_wire_fee,
result.account_balance,
result.available_balance,
TO_CHAR(result.sales_date, 'yyyy-MM-dd HH24:mi:ss.MS') AS sales_date
FROM (
-- Operation Type -> SELL, se genera a partir de transacciones de tipo CAPTURE, y AUTHORIZATION_AND_CAPTURE
SELECT 
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CAST('SELL' AS text) AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
t_payer_name AS payer_name,
t_payer_document_number AS payer_document_number,
t_payer_mail AS payer_mail,
t_payer_contact_phone AS payer_contact_phone,
t_payer_address AS payer_address,
t_payer_country AS payer_country,
COALESCE(ta.tx_moneda_iso_4217, '') AS currency_payment_request,
COALESCE(to_char(ta.tx_value, '999999999999999D99'), '') AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
gst.installments,
gst.promotion,
gst.authorization_code,
gst.operation_currency,
gst.operation_amount,
COALESCE(to_char(NULLIF((COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)), 0), '999999999999999D99'), '') AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_comision_payu, 0) + COALESCE(tm_tax_pol_commission, 0)), 0), '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_retention, 0) + COALESCE(tm_ica_retention, 0) + COALESCE(tm_renta_retention, 0) + COALESCE(tm_impuesto_renta_adicional, 0) + COALESCE(tm_iibb_merchant_commission, 0) + COALESCE(tm_iibb_payer_commission, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_tax_months_without_interest_commission, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_merchant_interest, 0) + COALESCE(tm_payer_interest, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_iva_merchant_interest, 0) + COALESCE(tm_iibb_merchant_interest, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CAST('' AS text) AS chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)- (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
gst.sales_date
FROM generic_sales_table gst
LEFT JOIN pps.transaccion_montos_adicionales ta ON ta.transaccion_id = gst.transaction_id 
WHERE t_tipo IN ('AUTHORIZATION_AND_CAPTURE', 'CAPTURE') 
AND pb_tipo_movimiento NOT IN ('REVERSION', 'CHARGEBACK', 'CHARGEBACK_FEE', 'FREEZE_FUND','UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> REVERSION, CHARGEBACK, PARTIAL_REFUND, se genera a partir de transacciones de tipo 'VOID', 'REFUND', 'CHARGEBACK' y 'PARTIAL_REFUND'
SELECT
gst.fecha_saldo_anterior AS date_operation_date,
gst.account_id,
gst.nombre,
CASE WHEN t_tipo in ('VOID', 'REFUND') THEN CAST('REFUND' AS text) ELSE t_tipo END
AS operation_type,
gst.description,
gst.reference,
gst.transaction_id,
gst.order_id,
gst.t_payer_name,
t_payer_document_number,
t_payer_mail,
t_payer_contact_phone,
t_payer_address,
t_payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
gst.payment_method,
gst.credit_card_number,
'' AS "instalments",
gst.promotion,
gst.authorization_code,
gst.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_iva_comision_payu, 0) + COALESCE(tm_reversion_tax_comision_payu, 0)), 0), '999999999999999D99'), '') AS "payu_fee_tax",
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_reteiva, 0) + COALESCE(tm_reversion_reteica, 0) + COALESCE(tm_reversion_reterenta, 0) + COALESCE(tm_reversion_impuesto_renta_adicional, 0) + COALESCE(tm_reversion_comision_iibb_comercio, 0) + COALESCE(tm_reversion_comision_iibb_pagador, 0)), 0), '999999999999999D99'), '') AS retentions,
COALESCE(to_char(tm_reversion_comision_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_fee,
COALESCE(to_char(tm_reversion_tax_interes_sin_mes, '999999999999999D99'), '') AS months_without_interest_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_payer_reversion_intereses, 0) + COALESCE(tm_merchant_reversion_intereses, 0)), 0), '999999999999999D99'), '') AS interest,
COALESCE(to_char(NULLIF((COALESCE(tm_reversion_interes_iva_comercio, 0) + COALESCE(tm_reversion_interes_iibb_comercio, 0)), 0), '999999999999999D99'), '') AS interest_tax,
CASE WHEN t_tipo = 'CHARGEBACK' THEN COALESCE(to_char(tm_chargeback_fee, '999999999999999D99'), '') ELSE '' END as chargeback_fee,
to_char(gst.valor, '999999999999999D99') AS net_amount,
to_char(gst.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(gst.valor + pb_saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM 
generic_sales_table gst
WHERE t_tipo IN ('VOID', 'REFUND', 'CHARGEBACK', 'PARTIAL_REFUND') 
AND pb_tipo_movimiento IN ('REVERSION', 'CHARGEBACK', 'PARTIAL_REFUND')
AND fecha_saldo_anterior IS NOT NULL
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Type -> MONEY_TRANSFER, se genera a partir de transacciones de tipo 'CHARGEBACK'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER'
ELSE 'MONEY_TRANSFER' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER'
AND pb_tipo_movimiento = 'PAYMENT_ORDER'
UNION
-- Operation Type -> SUPPLIER_MONEY_TRANSFER_REFUND si tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' cd lo contrario
-- Operation Tyoe -> MONEY_TRANSFER_REFUND, se genera a partir de transacciones de tipo 'PAYMENT_ORDER_REVERSION'
SELECT 
pot.operation_date AS date_operation_date,
pot.account_id,
pot.nombre,
CASE 
WHEN pot.tipo_transferencia_orden_pago = 'SUPPLIER_PAYMENT' THEN 'SUPPLIER_MONEY_TRANSFER_REFUND'
ELSE 'MONEY_TRANSFER_REFUND' 
END AS operation_type,
'' AS description,
'' AS reference,
CAST(orden_pago_id AS varchar) AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
pot.operation_currency,
COALESCE(to_char(tm_reversion_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_reversion_comision_payu, '999999999999999D99'), '') AS payu_fee,
COALESCE(to_char(tm_reversion_iva_comision_payu, '999999999999999D99'), '') AS payu_fee_tax,
COALESCE(to_char(tm_iibb_payment_order_reversion_pol_commission, '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(na.valor, '999999999999999D99') AS net_amount,
to_char(na.valor + pb_saldo_anterior, '999999999999999D99') AS account_balance,
to_char(na.valor + pb_saldo_anterior - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM payments_order_table pot
INNER JOIN payment_order_reversion_net_amount na ON pot.orden_pago_id = na.documento_soporte
WHERE tm_tipo_documento_soporte = 'PAYMENT_ORDER_REVERSION'
AND pb_tipo_movimiento = 'PAYMENT_ORDER_REVERSION'
UNION
-- Operation Type -> PAYU_TRANSFER, se genera a partir de movimientos con tipo_documento_soporte = 'ACCOUNT_TRANSFER'
SELECT 
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id_origen,
tm.nombre,
'PAYU_TRANSFER' AS operation_type,
'' AS description,
'' AS reference,
tc.transferencia_cuenta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS "reserved_amount",
COALESCE(to_char(tm_comision_payu, '999999999999999D99'), '') AS payu_fee,
'' AS payu_fee_tax,
COALESCE(to_char(NULLIF((COALESCE(tm_transfer_retention, 0)), 0), '999999999999999D99'), '') AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
to_char(NULLIF((COALESCE(tm.valor, 0)), 0), '999999999999999D99') AS net_amount,
to_char(tm.valor  + mv.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm.valor  + mv.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.transferencia_cuenta tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.transferencia_cuenta_id AND tm.cuenta_id = tc.cuenta_id_origen
INNER JOIN movement_values mv ON mv.documento_soporte = tc.transferencia_cuenta_id AND mv.tipo_movimiento = 'ACCOUNT_TRANSFER' AND mv.fecha_creacion = tm.fecha_saldo_anterior
WHERE tm_tipo_documento_soporte = 'ACCOUNT_TRANSFER'
UNION
-- Operation Type -> DISCRETIONAL_MOVEMENT, se genera a partir de movimientos con tipo_documento_soporte = 'DISCRETIONARY'
SELECT 
tm.fecha_creacion date_operation_date,
CAST(md.cuenta_id AS int) AS account_id,
tm.nombre,
'DISCRETIONAL_MOVEMENT' AS operation_type,
md.descripcion AS description,
'' AS reference,
md.movimiento_discrecional_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(tm_valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0) - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)  ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.movimiento_discrecional md
INNER JOIN transposed_movements tm ON tm.documento_soporte = md.movimiento_discrecional_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = md.movimiento_discrecional_id AND pb.tipo_movimiento IN ('DISCRETIONARY') 
WHERE tm.tm_tipo_documento_soporte = 'DISCRETIONARY'
UNION
-- Operation Type -> PAYMENT_CARDS, se genera a partir de movimientos con tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
SELECT
tm.fecha_creacion AS date_operation_date,
tc.cuenta_id AS account_id,
tm.nombre,
'PAYMENT_CARDS' AS operation_type,
'' AS description,
'' AS reference,
tc.solicitud_tarjeta_id AS transaction_id, 
'' AS order_id,
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(valor, '999999999999999D99'), '') AS operation_amount,
'' AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
COALESCE(to_char(valor, '999999999999999D99'), '') AS net_amount,
to_char(tm_valor + pb.saldo_anterior, '999999999999999D99') AS account_balance,
to_char(tm_valor + pb.saldo_anterior + COALESCE(tm_reserve_fund, 0) + COALESCE(tm_release_reserve_fund, 0)  -(COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0) ), '999999999999999D99') AS available_balance,
NULL AS sales_date
FROM pps.solicitud_tarjeta_cobranza tc
INNER JOIN transposed_movements tm ON tm.documento_soporte = tc.solicitud_tarjeta_id
INNER JOIN movement_prev_balance pb ON pb.documento_soporte = tc.solicitud_tarjeta_id AND pb.tipo_movimiento IN ('PAYMENT_CARD_REQUEST') 
WHERE tm.tm_tipo_documento_soporte = 'PAYMENT_CARD_REQUEST'
UNION
-- Operation type -> FREEZE si tipo_movimiento = 'FREEZE_FUND'
-- Operation type -> RELEASE RESERVE si tipo_movimiento = 'RELEASE_RESERVE_FUND'
-- Operation type -> UNFREEZE si no se cumplen las anteriores
SELECT
mv.fecha_creacion AS date_operation_date,
mv.cuenta_id,
mv.nombre,
CASE 
WHEN tipo_movimiento = 'FREEZE_FUND' THEN 'FREEZE' 
WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN 'RELEASE RESERVE'
ELSE 'UNFREEZE'
END AS operation_type,
'' AS description,
'' AS reference,
transaccion_id,
CAST(orden_id AS varchar),
'' AS payer_name,
'' AS payer_document_number,
'' AS payer_mail,
'' AS payer_contact_phone,
'' AS payer_address,
'' AS payer_country,
'' AS currency_payment_request,
'' AS amount_payment_request,
'' AS payment_method,
'' AS credit_card_number,
'' AS instalments,
'' AS promotion,
'' AS authorization_code,
moneda_iso_4217 AS operation_currency,
COALESCE(to_char(mv.valor, '999999999999999D99'), '') AS operation_amount,
COALESCE(to_char(NULLIF((CASE WHEN tipo_movimiento = 'RELEASE_RESERVE_FUND' THEN valor ELSE 0 END ), 0), '999999999999999D99'), '') AS reserved_amount,
'' AS payu_fee,
'' AS payu_fee_tax,
'' AS retentions,
'' AS months_without_interest_fee,
'' AS months_without_interest_tax,
'' AS interest,
'' AS interest_tax,
'' AS chargeback_fee,
'' AS net_amount,
to_char(saldo_anterior, '999999999999999D99') AS account_balance,
to_char(saldo_anterior + COALESCE(mv.valor, 0)  - (COALESCE(saldo_congelado_anterior, 0)+ COALESCE(saldo_reserva_anterior, 0)), '999999999999999D99') AS available_balance,
t.fecha_creacion AS sales_date
FROM 
transactions t
INNER JOIN movement_values mv ON t.transaccion_id = mv.documento_soporte AND tipo_movimiento IN ('FREEZE_FUND', 'UNFREEZE_FUND', 'RELEASE_RESERVE_FUND')
) as result order by date_operation_date, account_balance;
""",{'first_date':first_date,'last_date':last_date})
    monthlyreport=pd.DataFrame(cursor.fetchall())
    return monthlyreport