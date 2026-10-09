# PolyMarket Reverse Regression: 9 Ekim 2026 ?lerleme ve Kan?t Raporu

## S?n?rlar ve durum

- Yerel ?al??ma a?ac?: `C:\Users\acer\Desktop\Polymarket-Engine-reverse-regression`
- Branch: `forensic/reverse-regression-isolation`
- K?k Git commit'i: `21a195a6809e6d7eaf8b691dba65cf8ce3edda6f`
- Test kayna??: Docker image'lar?ndan al?nan **dokuz ger?ek derlenmi? kaynak haritas?**.
- **Production rollback, deploy, restart, prod DB yazma i?lemi YAPILMADI.** Branch GitHub'a g?nderilmedi, PR a??lmad?.
- G?ncel s?r?m `08_current` ?al??ma a?ac?na geri y?klendi.
- Testler ger?ek para/emir g?nderimi i?ermeyen **izole PAPER** d?zeninde y?r?t?ld?.

## Kan?t 1: S?r?m temelli uyum testleri

| S?r?m | Sonu? |
|---|---:|
| `00_safe` Guardian65 | **47 / 47 PASS** |
| `08_current` | **47 / 47 PASS** |
| Python hata g?zlemcisi | **9 / 9 PASS** |

Eski test beklentileri image a?amas?na sabitlendi. ?nemli d?zeltme: **Guardian EARLY_RECOVERY_WATCH_MS = 65000** de?eri Guardian65'in kendisinde de bulunmaktad?r. Dolay?s?yla Step3 veya sonraki s?r?mlerle gelen bir de?i?iklik de?ildir.

Dikkat: Bu testler canl? reference/CLOB kopmalar?n? yeniden ?retmiyor.

## Kan?t 2: Ba??ms?z PostgreSQL

Yerel bilgisayarda `pm-forensic-pg-20261009` ad?yla **ayr? Docker container** olu?turuldu. Ba?lant? yaln?zca `127.0.0.1` ?zerinden y?nlendiriliyor, CPU s?n?r? 0,5 vCPU ve RAM s?n?r? 768 MB. Postgres ?ema migration, sa?l?k kontrol?, journal append ve ayn? olay kimli?iyle tekrar append testleri ba?ar?yla ge?ti.

Production veritaban?na ba?lan?lmad?. Test giri? bilgileri `.gitignore` kapsam?ndaki `evidence/private/` klas?r?nde tutuldu, Git'e al?nmad?.

## Kan?t 3: Yerel ger?ek runtime ve a? engeli

G?ncel derlenmi? engine kendi ba??ms?z PostgreSQL'iyle **65 saniye** ?al??t?r?ld?. `/health` ve `/ready` yan?tlar? al?nd?. `database=UP`, `overloadState=NORMAL`, ancak `qualifiedMarketCount=0` ve `polymarketWs=STOPPED` idi.

Ba?lang?? logu: `fetch failed: self-signed certificate`. Do?rudan Node HTTPS kontrol?nde:
- Varsay?lan CA: `DEPTH_ZERO_SELF_SIGNED_CERT`
- Windows sistem CA ile: `ERR_TLS_CERT_ALTNAME_INVALID`

Bu nedenle Polymarket Gamma market ke?fi g?venli bi?imde ger?ekle?tirilemedi. **TLS do?rulamas? kapat?lmad?; sahte veya uyu?mayan sertifika kabul edilmedi.** S?f?r nitelikli marketli 65 saniyelik ko?u `INCONCLUSIVE_NO_TARGET` olarak kaydedildi. Bu ko?udan reference kopmas?n?n bulunmad??? sonucunu ??karamay?z.

## Kan?t 4: Kontrol edilen sentetik CPU y?k? (A-B-A)

?zole market adapter'? ve aktif PAPER pozisyonuyla, **100 market snapshot/s**, her ko?uda 300 snapshot (ek 150 ?s?nma) ve bellek-i?i journal:

| S?r?m | Event-loop p99 | Callback p99 | CPU / snapshot | Kuyruk max |
|---|---:|---:|---:|---:|
| G?ncel 1 | 20,07 ms | 7,93 ms | 4,53 ms | 3 |
| Guardian65 | 150,08 ms | 15,06 ms | 7,45 ms | 18 |
| G?ncel 2 | 18,02 ms | 5,78 ms | 4,27 ms | 3 |

Bu **tek Guardian65 ?rneklemi** ?zerine genelleme yap?lmaz; makine Windows, 16 vCPU ve farkl? a?/scheduling ko?ullar?na sahip. Ancak eski image'a toplu rollback'in performans kayb? yaratabilece?i kayda al?nm??t?r.

## Kan?t 5: Step2 ? Step3 ? Identity ? G?ncel callback y?k?

Ayn? sentetik tek-market, aktif PAPER pozisyonu, **100 snapshot/s / 200 snapshot** kar??la?t?rmas?:

| Image a?amas? | Tamamlanan | Event-loop p99 | Callback p99 | Eski A/B ?a?r?s? | Eski A/B senkron toplam |
|---|---:|---:|---:|---:|---:|
| Step2 | 200/200 | 17,07 ms | 5,00 ms | 200 | 2,0 ms |
| Step3 | 200/200 | 16,51 ms | 4,20 ms | 200 | 3,1 ms |
| Identity | 200/200 | 16,65 ms | 5,56 ms | 200 | 4,0 ms |
| G?ncel | 200/200 | 16,89 ms | 4,48 ms | 200 | 3,5 ms |

Ayn? ko?uda `observeGuardianSnapshot` her a?amada 200 kez ?al??t?; toplam senkron s?re yakla??k **326?355 ms** idi. Bu saya?lar i? i?e ?a?r?labildi?i i?in toplamlar? toplanmamal?d?r.

**Kritik s?n?r:** `executeRiskReductionArbitration = 0`; yani bu ?l??m Step3'?n deadline sonras? ger?ek taker/arbitration yolunu kapsamaz. Bu sonu? Step3 kaynakl? risk bug'? olmad???n? kan?tlamaz. Ayr?ca CPU ko?usunda PostgreSQL yerine MemoryEventJournal kullan?l?r.

## Hata g?zlemcisi

Ba??ms?z Python g?zlemci a?a??dakileri tan?yacak ?ekilde haz?rland?:
- Tek saniyede 7+ ayr? reference kapan???.
- Reference ile CLOB stale aras?nda 20ms ili?ki.
- PostgreSQL `CONNECT_TIMEOUT`.
- Loglanan kritik `FAIL_CLOSED`.

G?zlemci hata ?ncesi 120 saniyeye kadar s?n?rl? kan?t tutar. Hata sonras? 250ms kay?t toplay?p 8. asset veya sonradan gelen CLOB kayd?n? ka??rmama davran??? da test edildi. **9/9 ?nite testi ba?ar?l?.** ?zole PAPER ?al??t?rma i?in yerel PostgreSQL host/path/username, mod ve Node environment g?venlik kontrolleri eklendi; production DB URL'si reddediliyor.

## Hen?z ??z?lemeyenler ve sonraki zorunlu ko?ul

1. **?lk ger?ek toplu WebSocket kopmas?n?n sebebi hen?z tespit edilmedi.**
2. Yerel Windows ba?lant?s? TLS hostname do?rulamas?nda tak?ld??? i?in ger?ek Polymarket piyasa ak??? bu ortamda test edilemiyor.
3. ?nceki soaklar?n ham WebSocket mesaj ak??? mevcut de?il; tarih? olay? birebir replay etmek m?mk?n de?il.
4. Baz? ge?mi? CLI kaynaklar? (`operator-report.ts`) g?ncel derlenmi? image'dan kald?r?lm?? `source-seal.ts` dosyas?n? h?l? referansl?yor. Bu nedenle **tam workspace typecheck** t?m giri? noktalar?nda temiz de?ildir. Derlenmi? ana engine giri? noktas?n?n ba??ms?z typecheck'i PASS, ger?ek `esbuild` engine derlemesi PASS.
5. Reverse regression'?n canl? hatay? eleyebilmesi i?in ger?ek market ak???na eri?en **tamamen izole** ortam veya yeni, kurallara uygun ham feed kay?tlar? gerekir. Ayr? ?retim-d??? ortam haz?r olmadan yap?lan k?sa, hatas?z testlere `PASS` etiketi verilmez.
6. Yeni bir VPS container'?, VPS a? canary'si veya production'a herhangi bir deploy i?in kullan?c?n?n **ayr?ca a??k onay?** gerekir.

## ?leriye d?n?k deney kural?

- ?nce g?ncel image'da *ayn? y?kte* hedef ar?zay? ?ret.
- Sonraki a?amada tek de?i?iklik grubunu kald?r.
- Her a?amada ayn? giri? trafi?i/rate/seed, zaman s?n?r?, independent DB, heartbeat/fault kay?tlar?.
- Ar?za kayboldu?unda o par?ay? geri ekleyerek A-B-A do?rula.
- Hata hi? olu?mad?ysa stat? `INCONCLUSIVE`, asla `FIXED` de?il.

## Kaynak ve komutlar

- `research/reverse_regression/stage.py --stage 08_current --verify`
- `research/reverse_regression/fault_watch.py`
- `research/reverse_regression/run_local_paper.py`
- `research/reverse_regression/local_pg_smoke.ts`
- `research/reverse_regression/runs/pace_summary.json` (Git-ignore, yerel)
- `research/reverse_regression/runs/branch_comparison.json` (Git-ignore, yerel)
- `research/reverse_regression/runs/current-runtime-smoke/incident.json` (Git-ignore, yerel)
