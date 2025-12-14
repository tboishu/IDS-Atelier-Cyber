"use client";

import React, { useEffect, useState } from "react";
import dynamic from "next/dynamic";
import Image from "next/image";
import Link from "next/link";

const Bar = dynamic(() => import("react-chartjs-2").then(mod => mod.Bar), { ssr: false });
const Line = dynamic(() => import("react-chartjs-2").then(mod => mod.Line), { ssr: false });

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  PointElement,
  LineElement,
} from "chart.js";
import "chartjs-adapter-date-fns";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  PointElement,
  LineElement
);

interface EventData {
  _id?: string;
  titre: string;
  date: string;
  recommendation?: string;
  ip_source?: string;
  receivedAt?: string;
}

type TimeGrouping = "jour" | "heure";

// Fonction utilitaire pour formater la date au format DD/MM/YYYY
function formatDateFr(dateRaw: any) {
  const d = new Date(dateRaw);
  if (isNaN(d.getTime())) return '';
  // Ajout heure/minute en format français 2 chiffres
  return d.toLocaleDateString('fr-FR') + ' ' + d.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });
}

export default function Home() {
  const [events, setEvents] = useState<EventData[]>([]);
  const [timeGroup, setTimeGroup] = useState<TimeGrouping>("jour");

  useEffect(() => {
    async function fetchEvents() {
      const res = await fetch("/api/webhook");
      const data = await res.json();
      if (data.ok) setEvents(data.events);
      else setEvents([]);
    }
    fetchEvents();
    const interval = setInterval(fetchEvents, 30000); // Rafraîchir toutes les 30 secondes
    return () => clearInterval(interval);
  }, []);

  // Calcul pour le graphique 1 = nombre événements dans le temps
  function computeTimeSeries(group: TimeGrouping) {
    const counts: Record<string, number> = {};
    events.forEach(ev => {
      const raw = ev.date || ev.receivedAt || Date.now();
      const d = new Date(raw);
      let key;
      if (group === "jour") key = d.toISOString().slice(0, 10); // YYYY-MM-DD
      else key = d.toISOString().slice(0, 13); // YYYY-MM-DDTHH
      counts[key] = (counts[key] || 0) + 1;
    });
    // Trie par date croissante
    const labels = Object.keys(counts).sort();
    return {
      labels,
      data: labels.map(l => counts[l])
    };
  }

  // Calcul pour le graphique 2 = barres par titre
  function computeTitleBars() {
    const d: Record<string, number> = {};
    events.forEach(ev => {
      d[ev.titre] = (d[ev.titre] || 0) + 1;
    });
    const labels = Object.keys(d).sort((a, b) => d[b] - d[a]);
    return {
      labels,
      data: labels.map(l => d[l])
    };
  }

  // Statistiques
  const uniqueIPs = new Set(events.filter(e => e.ip_source).map(e => e.ip_source)).size;
  const eventsWithRecommendation = events.filter(e => e.recommendation).length;
  const lastEvent = events.length > 0 ? events[0] : null;

  // Préparation data graphiques
  const timeData = computeTimeSeries(timeGroup);
  const barsData = computeTitleBars();

  return (
    <main className="min-h-screen bg-gradient-to-br from-zinc-900 via-zinc-950 to-black text-zinc-100 p-6 md:p-10">
      {/* En-tête avec titre accrocheur */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <Image 
            src="/logo.png" 
            alt="Logo IDS ParisBrest" 
            width={60} 
            height={60} 
            className="rounded-lg"
            priority
          />
          <div className="flex items-center gap-3">
            <h1 className="text-4xl md:text-5xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
              Dashboard IDS ParisBrest
            </h1>
          </div>
        </div>
        <div className="flex items-center justify-between flex-wrap gap-4">
          <p className="text-zinc-400 text-lg">
            Surveillance en temps réel des événements de sécurité détectés par votre système IDS
          </p>
          <Link 
            href="/modele-ia"
            className="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-blue-500 to-purple-500 hover:from-blue-600 hover:to-purple-600 text-white font-semibold rounded-lg shadow-lg transition-all duration-200 transform hover:scale-105"
          >
            <span>🤖</span>
            <span>Modèle IA</span>
          </Link>
        </div>
      </div>

      {/* Cartes de statistiques */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-blue-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Total d'événements</span>
            <span className="text-2xl">📊</span>
          </div>
          <div className="text-3xl font-bold text-blue-400">{events.length}</div>
          <p className="text-zinc-500 text-xs mt-2">Événements enregistrés</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-purple-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">IP sources uniques</span>
            <span className="text-2xl">🌐</span>
          </div>
          <div className="text-3xl font-bold text-purple-400">{uniqueIPs}</div>
          <p className="text-zinc-500 text-xs mt-2">Adresses IP distinctes</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-green-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Recommandations</span>
            <span className="text-2xl">💡</span>
          </div>
          <div className="text-3xl font-bold text-green-400">{eventsWithRecommendation}</div>
          <p className="text-zinc-500 text-xs mt-2">Événements avec conseils</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-yellow-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Dernier événement</span>
            <span className="text-2xl">⏰</span>
          </div>
          <div className="text-lg font-bold text-yellow-400">
            {lastEvent ? formatDateFr(lastEvent.date || lastEvent.receivedAt) : 'Aucun'}
          </div>
          <p className="text-zinc-500 text-xs mt-2">Dernière détection</p>
        </div>
      </div>

      {/* Graphiques */}
      <div className="mb-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-xl font-bold text-zinc-100 mb-1">📈 Évolution temporelle</h2>
              <p className="text-zinc-400 text-sm">Suivi des événements dans le temps</p>
            </div>
            <select 
              className="bg-zinc-900 border border-zinc-600 text-zinc-100 px-3 py-2 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" 
              value={timeGroup} 
              onChange={e => setTimeGroup(e.target.value as TimeGrouping)}
            >
              <option value="jour">Par jour</option>
              <option value="heure">Par heure</option>
            </select>
          </div>
          <Line
            options={{
              responsive: true,
              maintainAspectRatio: true,
              plugins: {
                legend: { display: false },
                title: { display: false },
              },
              scales: {
                x: { 
                  type: 'category', 
                  title: { display: true, text: timeGroup === "jour" ? "Jour" : "Heure", color: '#a1a1aa' },
                  ticks: { color: '#71717a' },
                  grid: { color: '#3f3f46' }
                },
                y: { 
                  title: { display: true, text: "Nombre d'événements", color: '#a1a1aa' }, 
                  beginAtZero: true,
                  ticks: { color: '#71717a' },
                  grid: { color: '#3f3f46' }
                },
              },
            }}
            data={{
              labels: timeData.labels,
              datasets: [
                {
                  label: "Événements",
                  data: timeData.data,
                  borderColor: "#60a5fa",
                  backgroundColor: "rgba(96, 165, 250, 0.1)",
                  fill: true,
                  tension: 0.4,
                },
              ],
            }}
          />
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg">
          <div className="mb-4">
            <h2 className="text-xl font-bold text-zinc-100 mb-1">📊 Répartition par type</h2>
            <p className="text-zinc-400 text-sm">Types d'événements les plus fréquents</p>
          </div>
          <Bar
            options={{
              indexAxis: 'y',
              responsive: true,
              maintainAspectRatio: true,
              plugins: {
                legend: { display: false },
                title: { display: false },
              },
              scales: {
                x: { 
                  title: { display: true, text: 'Nombre d\'événements', color: '#a1a1aa' }, 
                  beginAtZero: true,
                  ticks: { color: '#71717a' },
                  grid: { color: '#3f3f46' }
                },
                y: { 
                  title: { display: true, text: 'Type d\'événement', color: '#a1a1aa' },
                  ticks: { color: '#71717a' },
                  grid: { color: '#3f3f46' }
                }
              }
            }}
            data={{
              labels: barsData.labels,
              datasets: [
                {
                  label: "Événements",
                  data: barsData.data,
                  backgroundColor: "rgba(168, 85, 247, 0.8)",
                  borderColor: "#a855f7",
                  borderWidth: 1,
                },
              ],
            }}
          />
        </div>
      </div>

      {/* Tableau des événements */}
      <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl shadow-lg overflow-hidden">
        <div className="p-6 border-b border-zinc-700">
          <h2 className="text-xl font-bold text-zinc-100 mb-1">📋 Journal des événements</h2>
          <p className="text-zinc-400 text-sm">Liste détaillée de tous les événements détectés par l'IDS</p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-zinc-700">
            <thead className="bg-zinc-900/50">
              <tr>
                {events[0] && Object.keys(events[0]).filter(key => key !== '_id').map((key) => (
                  <th key={key} className="px-6 py-4 text-left text-xs font-semibold text-zinc-300 uppercase tracking-wider">
                    {key === 'titre' ? '🔖 Titre' : key === 'date' ? '📅 Date' : key === 'ip_source' ? '🌐 IP Source' : key === 'recommendation' ? '💡 Recommandation' : key}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-700">
              {events.map((event, idx) => (
                <tr key={event.date + idx} className="hover:bg-zinc-800/50 transition-colors">
                  {Object.entries(event).filter(([k]) => k !== '_id').map(([key, value], i) => (
                    <td key={i} className="px-6 py-4 text-sm text-zinc-300 max-w-[300px]">
                      {['date', 'receivedAt'].includes(key) && value ? (
                        <span className="font-mono">{formatDateFr(value)}</span>
                      ) : typeof value === 'object' ? (
                        <pre className="text-xs whitespace-pre-wrap max-w-[400px] overflow-x-auto text-zinc-400 bg-zinc-900/50 p-2 rounded">{JSON.stringify(value, null, 2)}</pre>
                      ) : key === 'ip_source' ? (
                        <span className="font-mono text-blue-400">{String(value)}</span>
                      ) : key === 'recommendation' ? (
                        <span className="text-green-400">{String(value)}</span>
                      ) : (
                        <span>{String(value)}</span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
              {events.length === 0 && (
                <tr>
                  <td colSpan={5} className="py-12 text-center">
                    <div className="text-zinc-500 text-lg">🔍 Aucun événement à afficher pour le moment</div>
                    <p className="text-zinc-600 text-sm mt-2">Les événements détectés par l'IDS apparaîtront ici</p>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Footer avec info de rafraîchissement */}
      <div className="mt-6 text-center text-zinc-500 text-sm">
        <p>🔄 Mise à jour automatique toutes les 30 secondes</p>
      </div>
    </main>
  );
}
