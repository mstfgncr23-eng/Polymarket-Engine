# PolyMarket Reverse Regression: VPS Izole Test Sonuclari (2026-10-09)

## Status: ilk ariza yeniden uretildi, KOD REGRESYONU HENUZ IZOLE EDILMEDI

Bu calisma yalnizca izin verilen test container'larina dokundu. Production engine,
production PostgreSQL, live trading, DB migration ve rollback uygulanmadi. VPS'te
calisan production container `polymarket-engine-no-seal` degistirilmedi.

### Test ortami
- VPS Ubuntu, 2 vCPU, 3.7 GiB RAM.
- Tamamen ayri `pm-forensic-net-20261009` Docker network'u.
- PAPER, `GROK_ACTIVATION_PROVIDER=noop`, 40 kalifiye market, **MEMORY journal**.
- Real Gamma, CLOB ve Binance API'sine ayni VPS agindan erisim.
- Wallet/production token/production DB baglantisi yok.
- Test container'lari `--restart no`, RAM 512 MiB, no host ports, bounded Docker logs.
- CPU hard quota deneyleri dogal kaynak yetersizligini taklit eder, neden sonuclari icin
  production benzerligi **tek basina saglamaz**.

## Gercek gozlemler

| Isim | Kaynak politikasi | Sure | 7+ reference toplu kopma | Tekil sinyaller |
|---|---|---:|---|---:|
| Current Q0.30 | 0.30 vCPU quota, 512 MiB | ilk ariza | **VAR**, 7 asset 75ms | 41 sinyal |
| Guardian65 Q0.30 | 0.30 vCPU quota, 512 MiB | 81.28 s | **VAR**, 8 asset 4ms | 10 sinyal |
| Current Q0.65 | CPU1 pin, 0.65 vCPU quota | 172.4 s | Gozlenmedi (INCONCLUSIVE) | 38 sinyal |
| Current CPU1 soft | CPU1 pin, shares 128, **no quota** | 240.7 s | Gozlenmedi (INCONCLUSIVE) | 3 sinyal |

Q0.30 current ilk toplu kopma: **2026-10-09T18:56:09.188000+00:00**,
assetler: BNB, BTC, DOGE, ETH, HYPE, SOL, XRP. Production ayni zaman +-2sn icinde
referans/CLOB karsiligi gozlenmedi.

Q0.30 Guardian65 kopma: **2026-10-09T19:00:26.377000+00:00**,
assetler: BNB, BTC, DOGE, ETH, HYPE, SOL, XRP, ZEC. Production ayni
zaman +-2sn icinde referans/CLOB karsiligi gozlenmedi.

Q0.65 current cgroup: `nr_periods=1948`,
`nr_throttled=1662` (**%85.3 throttle edilen periyot**),
`throttled_usec=53022296`.
Soft shares current cgroup: `nr_throttled=0`,
`cpu.max=max 100000`. Bu inceleme, **hard CPU limitli staging kosularinin**
hata mekanizmasini degistirebildigini gosteriyor. Production arizasinin
kesin nedeninin CPU kotasi oldugu anlamina GELMEZ.

### Production FAIL_CLOSED: bu testlerden once

Production Docker loglarindaki ilk ilgili kritik hata:
**2026-10-09T07:30:31.929Z**
`critical journal append failed`:
`PERSISTENCE_OUTCOME_UNKNOWN: write CONNECT_TIMEOUT polymarket-postgres:5432`.

Ilk `final experiment report blocked by unresolved source integrity`:
**2026-10-09T07:33:47.810Z**; benzeri uyari sonraki saatlerde surdu.
Bu testler ise ilk olarak yaklasik 18:50Z basladi. Dolayisiyla kritik
journal hatasi staging testlerinin baslamasindan **once** yasanmistir.
Production `/ready` read-only son kontrolde: status=ready, database=UP,
polymarketWs=CONNECTED, qualifiedMarketCount=40, **overloadState=FAIL_CLOSED**.
Bu kilit testler sirasinda kaldirilmadi, restart/rollback yapilmadi.

### Neden simdi dogrudan Step3'u suclayamiyoruz?
1. **Safe Guardian65, yapay 0.30 CPU kitliginda ayni hata sinifini verdi.**
2. Normal CPU oncelikli kosuda guncele ait hata **4 dakikada yeniden
   uretilemedi**; buna temiz surum onayi verilemez.
3. Bu VPS testleri **MEMORY journal, aktif paper pozisyonsuz** baslatildi.
   Tarihi productionda ise Guardian, durable DB ve gercek pozisyonlar vardi.
4. Ham WebSocket frame replay kaydimiz yok. Es zamanli feed kapanmalari
   dis ag, Node zamanlayici tikanmasi veya surecler-arasi etkiler olabilir.
5. Source map differanslarindan `reference-feed.ts` kodu degismedi, ama
   event-loop queue sirasi ve Guardian davranisi degistiginden endirekt
   etkilesim olasiligi devam ediyor.

### Dogrulanmis artilar
- Ayrilmis staging images: SAFE / Step2 / Step3 / identity / CPU variants / current.
- Yerel branch: `forensic/reverse-regression-isolation`.
- `vps_reverse_runner.py`: canli market baglantili izole PAPER onayli kosu
  ile staging image secimini ve **7+ reference** event-trigger durusunu otomatiklestirir.
  Varsayilan `--dry-run` modunda Docker'a dokunmaz.
- `test_vps_reverse_runner.py` ve `test_fault_watch.py`: toplam 16 unittest PASS.
- Konteynerler test sonu durduruldu; forensic DB container ve volume
  test icin korunuyor ama durdurulmus durumda.

### Aksiyon / yetki siniri
- **UYGULANMADI**: production restart, rollback, production DB veri duzeltmesi,
  `FAIL_CLOSED` reseti veya yeni trade.
- Kod regresyon izolasyonu icin gerekli sonraki adim: ayni trafik ve aktif Guardian
  pozisyonu altinda, normal CPU onceligiyle g?ncel surumde ger?ek hata tetiklemek;
  sonra tek image/degisiklik grubunu kaldirip A-B-A ile yeniden dogrulamak.
- Kritik PostgreSQL `CONNECT_TIMEOUT` icin ayrica **productiondan tamamen ayri,
  credential aktarimi guvenli cozulmus** PostgreSQL ile deney gereklidir.
- Kullanici production risk durumuyla ilgili ayri karar verene kadar
  `FAIL_CLOSED` degistirilmeyecektir.

## Kanit dosyalari (Git'e yuklenmedi)

`research/reverse_regression/evidence/private/` icinde:
- `first_fault_comparison.json`
- `guardian65_lowcpu_fault.json`
- `current65_result.json`
- `current_soft_result.json`

Origin remote'a yuklenirken raw log, sifre, DB export ve buyuk sourcemap dahil edilmemelidir.
