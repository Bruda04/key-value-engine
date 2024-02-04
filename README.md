# key-value-engine

## Authors
- [Marija Parezanin](https://github.com/marijaparezanin)
- [Marina Ivanovic](https://github.com/marina-ivanovic)
- [Luka Bradic](https://github.com/Bruda04)



## Operacije sa probabilističkim tipovima smo uneli u naš sistem na sledeći način:
 
Korisnik ne sme da stavlja whitespace karaktere kao vrednost ključa, jer imamo poseban regex koji se brine o tome da će zapisi operacije put biti isključivo tipa: put ključ vrednost
Samim tim smo osigurali da će se zapisi vezani za probabilističke tipove razlikovati od običnih zapisa jer se probabilističke strukture skladište sa ključem koji je oblika imeStrukture naziv. Korisnik pri kreiranju strukture unosi ime za skladištenje same strukture. U zavisnosti od strukture koja se kreira od korisnika će biti traženo da unese dodatne parametre potrebne za kreiranje i rad sa strukturom (preciznost, očekivani broj elemenata...). Korisniku nakon kreiranja objekta ostaju opcije rada sa strukturom a to su njeno brisanje, dodavanje u strukturu, ili zahtev provere te strukture za određenu vrednost.
Pozivi operacija su oblika: 
 - **(cms|bf|hll) make name**
 - **(cms|bf|hll) destroy name**
 - **(cms|bf|hll) put name value**
 - **(cms|bf) check name value**
- **hll check name**

Za skladištenje otiska teksta koristi se komanda fingerprint, a nad dva otiska se može izračunati simhash.
Pozivi funkcija su oblika:
 - **fingerprint name text**
 - **simhash fingerprintName fingerprintName**

 Sve make funkcije pozivaju writePath i upisuju serijalizovanu strukturu u sistem. Sve destroy funkcije pozivaju readPath radi nalaženja zapisa, postavljaju oznaku za deletet i preko writePath upisuju nazad u sistem. Sve check funkcije pozivaju readPath radi nalaženja zapisa, deserijalizuju strukturu i pozivaju odgovarajuću funkciju provere. Sve funkcije put deserijalizuju strukturu i pozivaju odgovarajuću funkciju dodavanja, i na kraju upisuju novu, izmenjenu, serijalizovanu verziju nazad u sistem pomoći writePath.




### Ograničenje stope pristupa

**Stopa pristupa predstavlja ograničavanje korisnika (ili grupe korisnika) da zaredo traže više zahteva.**

Način na koji smo se mi o tome brinuli je taj da u strukturi TokenBucket imamo poseban mehanizam čuvanja poslednjeg update-ovanja strukture (vraćanje broja tokena na najveći mogući, korisnički definisan) i čuvanje posebne vrednosti - refillCooldown - koju koristimo pri svakoj proveri da li je prošlo vreme (da li je prošao cooldown) od poslenjeg ažuriranja broja tokena.
- Zapise o samim zahtevima (logovima) čuvamo u sistemu, u obliku tokenLog + vreme kada je zahtev poslat. Pošto postoji razmak između tokenLog-a i vremena slanja zahteva, ove zapise, kao i zapise probabilističkih tipova, korisnik neće moći da get-uje, zbog same sintakse get operacije.




## Operacija sa iteratorima 

-Prefix iterate(prefix) / Range iterate(range) - oba iteratora se oslanjaju na individualne iteratore napravljene za memtabele i sst tabele. U memtabeli u zavisnosti od implementirane strukture postoji iterator za hashmapu, skiplistu i bstablo. Sva tri iteratora implementiraju interfejs koji zahtjeva funckije:

- **Valid() bool**
  - Provjerava da li je trenutni element nadjen

- **Next()**
  - Pomjera iterator, azurira trenutnu vrijednost iteratora.

- **Get()**
  - Vraca trenutnu poziciju (podatka).


-Svi individualni iteratori su prilagodjeni da rade i za prefix i za range, gdje se u zavisnosti od poziva funkcije odredjuju relevanti parametri zaustavljanja i pronalazenja.
-Menadzer memtabela i sstabele imaju funckije zaduzene za generisanje n iteratora, gdje je n broj memtabela, odnosno sstabela. Potom prefix i range su zaduzeni za to da prolaze kroz sve iteratore i nadju medju njima najmanji element koji ce biti vracen preko funckije Next. Individualni iteratori su sortirani. Pri biranju sljedeceg iterator provjerava uslove kojim omogucuje da pri nailasku na iste elemente uvijek bira one najnovije. Takodje ako naidje na elemente gdje je najnovija verzija izbrisana, iteratori tih elemenata ce ih samo preskociti. 

## Operacija sa iteratorima 

-Operacije skeniranja su iskoristile odgovarajuce iteratore. Pozivaju iterator broj stranica - 1 * velicina stranice puta, kako bi procitali stranice koje prethode onoj zahtjevanoj. Potom narednih velicina stranice puta inkrementiraju iterator i ispisu njegovu vrijednost. U slucaju da iterator nema podatke na toj stranici, ispisace prazno.

        
  
