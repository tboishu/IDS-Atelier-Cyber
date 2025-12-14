import { NextRequest, NextResponse } from 'next/server';
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI as string;
const dbName = process.env.MONGODB_DB || 'ids';

let cachedClient: MongoClient | null = null;

async function getClient() {
  if (cachedClient) {
    return cachedClient;
  }
  cachedClient = new MongoClient(uri);
  await cachedClient.connect();
  return cachedClient;
}

export async function POST(request: NextRequest) {
  try {
    const data = await request.json();

    // Champs attendus
    const { titre, date, recommendation, ip_source } = data;
    if (!titre || typeof titre !== 'string') {
      return NextResponse.json(
        { ok: false, error: 'Le champ "titre" est obligatoire et doit être une chaîne.' },
        { status: 400 }
      );
    }
    // Préparer l'objet à insérer
    const eventToInsert: any = {
      titre,
      date: date ? new Date(date) : new Date(),
      receivedAt: new Date(),
    };
    if (recommendation) eventToInsert.recommendation = recommendation;
    if (ip_source) eventToInsert.ip_source = ip_source;

    // Connexion et insertion dans la base
    const client = await getClient();
    const db = client.db(dbName);
    const result = await db.collection('events').insertOne(eventToInsert);
    return NextResponse.json({ ok: true, insertedId: result.insertedId });
  } catch (error) {
    console.error('Erreur dans le webhook:', error);
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : 'Erreur inconnue' },
      { status: 500 },
    );
  }
}

export async function GET() {
  try {
    const client = await getClient();
    const db = client.db(dbName);
    const events = await db.collection('events').find({}).sort({ receivedAt: -1 }).limit(200).toArray();
    return NextResponse.json({ ok: true, events });
  } catch (error) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : 'Erreur inconnue' }, { status: 500 });
  }
}
