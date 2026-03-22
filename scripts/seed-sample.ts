/**
 * Seed the Romanian Financial Regulation database with sample provisions for testing.
 *
 * Inserts provisions from ASF_Norme (capital markets), ASF_Instructiuni,
 * BNR_Regulamente (prudential requirements), and BNR_Norme sourcebooks.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["ASF_DB_PATH"] ?? "data/asf.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

interface SourcebookRow {
  id: string;
  name: string;
  description: string;
}

const sourcebooks: SourcebookRow[] = [
  {
    id: "ASF_NORME",
    name: "ASF Norme",
    description: "Norme emise de Autoritatea de Supraveghere Financiara privind piata de capital, asigurarile si pensiile private.",
  },
  {
    id: "ASF_INSTRUCTIUNI",
    name: "ASF Instructiuni",
    description: "Instructiuni ASF privind procedurile de autorizare, raportare si conformitate pentru participantii la piata.",
  },
  {
    id: "BNR_REGULAMENTE",
    name: "BNR Regulamente",
    description: "Regulamente emise de Banca Nationala a Romaniei privind cerintele prudentiale pentru institutiile de credit.",
  },
  {
    id: "BNR_NORME",
    name: "BNR Norme",
    description: "Norme BNR privind supravegherea bancara, managementul riscului si cerintele de capital.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

interface ProvisionRow {
  sourcebook_id: string;
  reference: string;
  title: string;
  text: string;
  type: string;
  status: string;
  effective_date: string;
  chapter: string;
  section: string;
}

const provisions: ProvisionRow[] = [
  {
    sourcebook_id: "ASF_NORME",
    reference: "ASF_NORME 15/2015 art.3",
    title: "Conditii de autorizare pentru societatile de servicii de investitii financiare",
    text: "Societatile de servicii de investitii financiare trebuie sa dispuna de capital initial suficient, conform cerintelor stabilite de ASF, si sa demonstreze ca actionarii semnificativi au o reputatie adecvata si o situatie financiara solida.",
    type: "norma",
    status: "in_force",
    effective_date: "2015-07-01",
    chapter: "I",
    section: "1",
  },
  {
    sourcebook_id: "ASF_NORME",
    reference: "ASF_NORME 15/2015 art.7",
    title: "Cerinte de capital pentru servicii de tranzactionare",
    text: "Societatile care presteaza servicii de tranzactionare in cont propriu trebuie sa mentina un nivel minim al capitalului propriu de cel putin echivalentul in lei al 730.000 euro. Capitalul propriu se calculeaza conform reglementarilor contabile aplicabile si se raporteaza trimestrial la ASF.",
    type: "norma",
    status: "in_force",
    effective_date: "2015-07-01",
    chapter: "II",
    section: "2",
  },
  {
    sourcebook_id: "ASF_NORME",
    reference: "ASF_NORME 39/2015 art.4",
    title: "Obligatii de raportare privind tranzactiile suspecte",
    text: "Participantii la piata de capital au obligatia de a raporta la ASF orice tranzactie care poate constitui abuz de piata, in termen de cel mult o zi lucratoare de la data la care au luat cunostinta de aceasta.",
    type: "norma",
    status: "in_force",
    effective_date: "2015-12-01",
    chapter: "III",
    section: "4",
  },
  {
    sourcebook_id: "ASF_NORME",
    reference: "ASF_NORME 4/2018 art.2",
    title: "Cerinte de conduita privind distributia produselor de investitii",
    text: "Distribuitorii de produse de investitii trebuie sa actioneze in interesul superior al clientilor. Aceasta obligatie impune furnizarea de informatii corecte, clare si neinselatoare, evaluarea adecvarii produsului cu profilul clientului si evitarea conflictelor de interese.",
    type: "norma",
    status: "in_force",
    effective_date: "2018-03-01",
    chapter: "I",
    section: "1",
  },
  {
    sourcebook_id: "ASF_INSTRUCTIUNI",
    reference: "ASF_INSTRUCTIUNI 2/2016 art.5",
    title: "Procedura de autorizare a ofertelor publice de valori mobiliare",
    text: "Emitentii care doresc sa efectueze o oferta publica initiala trebuie sa depuna la ASF un prospect intocmit in conformitate cu Regulamentul (UE) nr. 2017/1129. Prospectul va fi aprobat de ASF in termen de 20 de zile lucratoare de la data depunerii documentatiei complete.",
    type: "instructiune",
    status: "in_force",
    effective_date: "2016-06-01",
    chapter: "II",
    section: "5",
  },
  {
    sourcebook_id: "ASF_INSTRUCTIUNI",
    reference: "ASF_INSTRUCTIUNI 5/2019 art.3",
    title: "Instructiuni privind raportarea pozitiilor short",
    text: "Detinatorii de pozitii short nete semnificative in actiuni admise la tranzactionare pe o piata reglementata din Romania trebuie sa raporteze aceste pozitii la ASF in conformitate cu Regulamentul (UE) nr. 236/2012.",
    type: "instructiune",
    status: "in_force",
    effective_date: "2019-09-01",
    chapter: "I",
    section: "3",
  },
  {
    sourcebook_id: "BNR_REGULAMENTE",
    reference: "BNR_REGULAMENTE 5/2013 art.10",
    title: "Cerinte de capital sub Pilonul 1 — rata minima de solvabilitate",
    text: "Institutiile de credit trebuie sa mentina in permanenta o rata de solvabilitate de minimum 8%, calculata ca raport intre fondurile proprii si activele ponderate in functie de risc. BNR poate impune cerinte suplimentare de capital individual pentru institutiile care prezinta un profil de risc ridicat.",
    type: "regulament",
    status: "in_force",
    effective_date: "2013-10-01",
    chapter: "III",
    section: "10",
  },
  {
    sourcebook_id: "BNR_REGULAMENTE",
    reference: "BNR_REGULAMENTE 5/2013 art.15",
    title: "Amortizoare de capital",
    text: "Institutiile de credit trebuie sa mentina amortizoare de capital conform cerintelor Directivei CRD IV. Acestea includ amortizorul de conservare a capitalului de 2,5% din activele ponderate la risc, amortizorul anticiclic de capital stabilit trimestrial de BNR si amortizorii pentru institutiile de importanta sistemica.",
    type: "regulament",
    status: "in_force",
    effective_date: "2016-01-01",
    chapter: "IV",
    section: "15",
  },
  {
    sourcebook_id: "BNR_REGULAMENTE",
    reference: "BNR_REGULAMENTE 17/2012 art.6",
    title: "Cerinte privind lichiditatea — rata de acoperire a necesarului de lichiditate",
    text: "Institutiile de credit trebuie sa detina active lichide de inalta calitate suficiente pentru a acoperi iesirile nete de numerar pe o perioada de 30 de zile calendaristice in conditii de stres. Rata de acoperire a necesarului de lichiditate (LCR) trebuie sa fie de cel putin 100% in mod continuu.",
    type: "regulament",
    status: "in_force",
    effective_date: "2015-10-01",
    chapter: "II",
    section: "6",
  },
  {
    sourcebook_id: "BNR_NORME",
    reference: "BNR_NORME 6/2021 art.8",
    title: "Cerinte de guvernanta interna pentru institutiile de credit",
    text: "Institutiile de credit trebuie sa dispuna de un cadru solid de guvernanta interna, care sa includa o structura organizatorica clara cu linii de responsabilitate bine definite, procese eficiente de identificare, gestionare si monitorizare a riscurilor, si mecanisme adecvate de control intern.",
    type: "norma",
    status: "in_force",
    effective_date: "2021-03-01",
    chapter: "III",
    section: "8",
  },
];

const insertProvision = db.prepare(`
  INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertAll = db.transaction(() => {
  for (const p of provisions) {
    insertProvision.run(
      p.sourcebook_id, p.reference, p.title, p.text,
      p.type, p.status, p.effective_date, p.chapter, p.section,
    );
  }
});

insertAll();

console.log(`Inserted ${provisions.length} sample provisions`);

interface EnforcementRow {
  firm_name: string;
  reference_number: string;
  action_type: string;
  amount: number;
  date: string;
  summary: string;
  sourcebook_references: string;
}

const enforcements: EnforcementRow[] = [
  {
    firm_name: "SC Broker de Asigurare SRL",
    reference_number: "ASF-SA-10-2022",
    action_type: "fine",
    amount: 50000,
    date: "2022-05-15",
    summary: "Amenda aplicata pentru incalcarea obligatiilor de raportare periodica privind activitatea de intermediere in asigurari. Societatea nu a transmis rapoartele trimestriale in termenele legale.",
    sourcebook_references: "ASF_NORME 15/2015 art.3",
  },
  {
    firm_name: "Alpha Investment Advisors SA",
    reference_number: "ASF-SSIF-5-2023",
    action_type: "restriction",
    amount: 0,
    date: "2023-09-20",
    summary: "Restrictionarea activitatii de gestionare a portofoliilor individuale pentru incalcarea cerintelor de adecvare si compatibilitate conform MiFID II. Societatea nu a efectuat evaluarile necesare ale profilului de risc al clientilor.",
    sourcebook_references: "ASF_NORME 4/2018 art.2, ASF_INSTRUCTIUNI 2/2016 art.5",
  },
];

const insertEnforcement = db.prepare(`
  INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const insertEnforcementsAll = db.transaction(() => {
  for (const e of enforcements) {
    insertEnforcement.run(
      e.firm_name, e.reference_number, e.action_type, e.amount,
      e.date, e.summary, e.sourcebook_references,
    );
  }
});

insertEnforcementsAll();

console.log(`Inserted ${enforcements.length} sample enforcement actions`);

const provisionCount = (db.prepare("SELECT count(*) as cnt FROM provisions").get() as { cnt: number }).cnt;
const sourcebookCount = (db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as { cnt: number }).cnt;
const enforcementCount = (db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as { cnt: number }).cnt;
const ftsCount = (db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sourcebooks:          ${sourcebookCount}`);
console.log(`  Provisions:           ${provisionCount}`);
console.log(`  Enforcement actions:  ${enforcementCount}`);
console.log(`  FTS entries:          ${ftsCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
