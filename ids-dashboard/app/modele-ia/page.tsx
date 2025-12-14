"use client";

import React from "react";
import dynamic from "next/dynamic";
import Image from "next/image";
import Link from "next/link";

const Bar = dynamic(() => import("react-chartjs-2").then(mod => mod.Bar), { ssr: false });

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

// Données du BoxPlot
const boxplotData = {
  min: 20,
  q1: 23,
  median: 28,
  q3: 36,
  max: 47,
  outliers: [0]
};

// Données de l'histogramme
const histogramData = [
  0, 0, 0, 0, 0, 0, 0,
  20, 21, 22, 23, 24,
  28, 28, 28, 28, 28, 28, 28, 28, 28, 28, 28,
  30, 31, 32,
  35, 38,
  40, 41, 42, 42,
  45, 47, 48
];

// Fonction pour créer les données du BoxPlot
function prepareBoxPlotData() {
  // Calcul des intervalles pour l'histogramme
  const bins: Record<string, number> = {};
  histogramData.forEach(value => {
    const bin = Math.floor(value / 5) * 5; // Groupes de 5
    bins[bin] = (bins[bin] || 0) + 1;
  });

  return {
    // Données pour la boîte (box)
    boxData: [
      boxplotData.q1, // Q1
      boxplotData.median - boxplotData.q1, // Médiane - Q1
      boxplotData.q3 - boxplotData.median, // Q3 - Médiane
      boxplotData.max - boxplotData.q3, // Max - Q3
    ],
    // Positions pour les whiskers et outliers
    whiskerMin: boxplotData.min,
    whiskerMax: boxplotData.max,
    median: boxplotData.median,
    outliers: boxplotData.outliers,
  };
}

// Fonction pour préparer les données de l'histogramme
function prepareHistogramData() {
  const bins: Record<number, number> = {};
  histogramData.forEach(value => {
    bins[value] = (bins[value] || 0) + 1;
  });

  // Trouver le min et max pour afficher toutes les valeurs
  const minVal = Math.min(...histogramData);
  const maxVal = Math.max(...histogramData);
  
  // Créer un tableau avec toutes les valeurs entre min et max
  const allLabels: number[] = [];
  const allData: number[] = [];
  
  for (let i = minVal; i <= maxVal; i++) {
    allLabels.push(i);
    allData.push(bins[i] || 0);
  }

  return {
    labels: allLabels.map(k => k.toString()),
    data: allData,
  };
}

export default function ModeleIAPage() {
  const boxPlotData = prepareBoxPlotData();
  const histogramChartData = prepareHistogramData();

  // Calcul des statistiques
  const mean = histogramData.reduce((a, b) => a + b, 0) / histogramData.length;
  const sorted = [...histogramData].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)];
  const min = Math.min(...histogramData);
  const max = Math.max(...histogramData);

  return (
    <main className="min-h-screen bg-gradient-to-br from-zinc-900 via-zinc-950 to-black text-zinc-100 p-6 md:p-10">
      {/* En-tête avec titre accrocheur */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <Link href="/" className="hover:opacity-80 transition-opacity">
            <Image 
              src="/logo.png" 
              alt="Logo IDS ParisBrest" 
              width={60} 
              height={60} 
              className="rounded-lg"
              priority
            />
          </Link>
          <div className="flex items-center gap-3">
            <h1 className="text-4xl md:text-5xl font-bold bg-gradient-to-r from-blue-400 to-purple-400 bg-clip-text text-transparent">
              Modèle IA
            </h1>
          </div>
        </div>
        <p className="text-zinc-400 text-lg">
          Analyse des performances et des erreurs de reconstruction du modèle d'IA
        </p>
      </div>

      {/* Cartes de statistiques */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-blue-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Moyenne</span>
            <span className="text-2xl">📊</span>
          </div>
          <div className="text-3xl font-bold text-blue-400">{mean.toFixed(2)}</div>
          <p className="text-zinc-500 text-xs mt-2">Erreur moyenne</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-purple-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Médiane</span>
            <span className="text-2xl">📈</span>
          </div>
          <div className="text-3xl font-bold text-purple-400">{median}</div>
          <p className="text-zinc-500 text-xs mt-2">Erreur médiane</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-green-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Minimum</span>
            <span className="text-2xl">⬇️</span>
          </div>
          <div className="text-3xl font-bold text-green-400">{min}</div>
          <p className="text-zinc-500 text-xs mt-2">Erreur minimale</p>
        </div>

        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg hover:border-yellow-500 transition-colors">
          <div className="flex items-center justify-between mb-2">
            <span className="text-zinc-400 text-sm font-medium">Maximum</span>
            <span className="text-2xl">⬆️</span>
          </div>
          <div className="text-3xl font-bold text-yellow-400">{max}</div>
          <p className="text-zinc-500 text-xs mt-2">Erreur maximale</p>
        </div>
      </div>

      {/* Graphiques */}
      <div className="mb-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* BoxPlot */}
        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg">
          <div className="mb-4">
            <h2 className="text-xl font-bold text-zinc-100 mb-1">📦 BoxPlot des erreurs de reconstruction</h2>
            <p className="text-zinc-400 text-sm">Distribution des erreurs avec quartiles et valeurs aberrantes</p>
          </div>
          <div className="relative w-full" style={{ height: '400px' }}>
            <svg width="100%" height="100%" viewBox="0 0 800 400" preserveAspectRatio="xMidYMid meet" className="overflow-visible">
              {/* Calcul des positions dynamiques */}
              {(() => {
                const paddingLeft = 60;
                const paddingRight = 200;
                const paddingTop = 20;
                const paddingBottom = 20;
                const chartWidth = 800 - paddingLeft - paddingRight;
                const chartHeight = 400 - paddingTop - paddingBottom;
                const maxValue = Math.max(boxplotData.max, ...boxplotData.outliers);
                const minValue = Math.min(boxplotData.min, ...boxplotData.outliers);
                const valueRange = maxValue - minValue || 1;
                
                // Position centrale du boxplot (au milieu de la zone de graphique)
                const centerX = paddingLeft + chartWidth / 2;
                const boxWidth = 80;
                
                // Fonction pour convertir une valeur en position Y
                const valueToY = (val: number) => {
                  return paddingTop + chartHeight - ((val - minValue) / valueRange) * chartHeight;
                };
                
                return (
                  <>
                    {/* Axe Y */}
                    <line 
                      x1={paddingLeft} 
                      y1={paddingTop} 
                      x2={paddingLeft} 
                      y2={400 - paddingBottom} 
                      stroke="#71717a" 
                      strokeWidth="2" 
                    />
                    
                    {/* Étiquettes Y */}
                    {[0, 10, 20, 30, 40, 50].map((val) => {
                      if (val < minValue || val > maxValue) return null;
                      const y = valueToY(val);
                      return (
                        <g key={val}>
                          <line 
                            x1={paddingLeft - 5} 
                            y1={y} 
                            x2={paddingLeft} 
                            y2={y} 
                            stroke="#71717a" 
                            strokeWidth="1" 
                          />
                          <text 
                            x={paddingLeft - 10} 
                            y={y + 5} 
                            fill="#a1a1aa" 
                            fontSize="12" 
                            textAnchor="end"
                          >
                            {val}
                          </text>
                        </g>
                      );
                    })}

                    {/* Whisker inférieur */}
                    <line 
                      x1={centerX} 
                      y1={valueToY(boxplotData.min)} 
                      x2={centerX} 
                      y2={valueToY(boxplotData.q1)} 
                      stroke="#60a5fa" 
                      strokeWidth="3" 
                    />
                    <line 
                      x1={centerX - 15} 
                      y1={valueToY(boxplotData.min)} 
                      x2={centerX + 15} 
                      y2={valueToY(boxplotData.min)} 
                      stroke="#60a5fa" 
                      strokeWidth="3" 
                    />

                    {/* Boîte (Box) */}
                    <rect
                      x={centerX - boxWidth / 2}
                      y={valueToY(boxplotData.q3)}
                      width={boxWidth}
                      height={valueToY(boxplotData.q1) - valueToY(boxplotData.q3)}
                      fill="rgba(96, 165, 250, 0.3)"
                      stroke="#60a5fa"
                      strokeWidth="2"
                    />

                    {/* Médiane */}
                    <line
                      x1={centerX - boxWidth / 2}
                      y1={valueToY(boxplotData.median)}
                      x2={centerX + boxWidth / 2}
                      y2={valueToY(boxplotData.median)}
                      stroke="#a855f7"
                      strokeWidth="3"
                    />

                    {/* Whisker supérieur */}
                    <line 
                      x1={centerX} 
                      y1={valueToY(boxplotData.q3)} 
                      x2={centerX} 
                      y2={valueToY(boxplotData.max)} 
                      stroke="#60a5fa" 
                      strokeWidth="3" 
                    />
                    <line 
                      x1={centerX - 15} 
                      y1={valueToY(boxplotData.max)} 
                      x2={centerX + 15} 
                      y2={valueToY(boxplotData.max)} 
                      stroke="#60a5fa" 
                      strokeWidth="3" 
                    />

                    {/* Outliers */}
                    {boxplotData.outliers.map((outlier, idx) => (
                      <circle
                        key={idx}
                        cx={centerX}
                        cy={valueToY(outlier)}
                        r="6"
                        fill="#ef4444"
                        stroke="#dc2626"
                        strokeWidth="2"
                      />
                    ))}
                  </>
                );
              })()}

              {/* Légende */}
              <g transform="translate(620, 50)">
                <rect x="0" y="0" width="15" height="15" fill="rgba(96, 165, 250, 0.3)" stroke="#60a5fa" strokeWidth="1" />
                <text x="20" y="12" fill="#a1a1aa" fontSize="12">Quartiles (Q1-Q3)</text>
                
                <line x1="0" y1="25" x2="15" y2="25" stroke="#a855f7" strokeWidth="2" />
                <text x="20" y="28" fill="#a1a1aa" fontSize="12">Médiane</text>
                
                <line x1="7.5" y1="40" x2="7.5" y2="50" stroke="#60a5fa" strokeWidth="2" />
                <text x="20" y="48" fill="#a1a1aa" fontSize="12">Whiskers (Min-Max)</text>
                
                <circle cx="7.5" cy="62" r="4" fill="#ef4444" stroke="#dc2626" strokeWidth="1" />
                <text x="20" y="66" fill="#a1a1aa" fontSize="12">Outliers</text>
              </g>

              {/* Valeurs */}
              <g transform="translate(620, 150)">
                <text x="0" y="0" fill="#a1a1aa" fontSize="11" fontWeight="bold">Statistiques:</text>
                <text x="0" y="20" fill="#a1a1aa" fontSize="10">Min: {boxplotData.min}</text>
                <text x="0" y="35" fill="#a1a1aa" fontSize="10">Q1: {boxplotData.q1}</text>
                <text x="0" y="50" fill="#a1a1aa" fontSize="10">Médiane: {boxplotData.median}</text>
                <text x="0" y="65" fill="#a1a1aa" fontSize="10">Q3: {boxplotData.q3}</text>
                <text x="0" y="80" fill="#a1a1aa" fontSize="10">Max: {boxplotData.max}</text>
              </g>
            </svg>
          </div>
        </div>

        {/* Histogramme */}
        <div className="bg-zinc-800/50 backdrop-blur-sm border border-zinc-700 rounded-xl p-6 shadow-lg">
          <div className="mb-4">
            <h2 className="text-xl font-bold text-zinc-100 mb-1">📊 Histogramme des erreurs de construction</h2>
            <p className="text-zinc-400 text-sm">Fréquence des erreurs par valeur</p>
          </div>
          <Bar
            options={{
              responsive: true,
              maintainAspectRatio: true,
              plugins: {
                legend: { display: false },
                title: { display: false },
              },
              scales: {
                x: { 
                  title: { display: true, text: 'Valeur d\'erreur', color: '#a1a1aa' },
                  ticks: { color: '#71717a', maxRotation: 45, minRotation: 45 },
                  grid: { color: '#3f3f46' }
                },
                y: { 
                  title: { display: true, text: 'Fréquence', color: '#a1a1aa' }, 
                  beginAtZero: true,
                  ticks: { color: '#71717a', stepSize: 1 },
                  grid: { color: '#3f3f46' }
                },
              },
            }}
            data={{
              labels: histogramChartData.labels,
              datasets: [
                {
                  label: "Fréquence",
                  data: histogramChartData.data,
                  backgroundColor: "rgba(168, 85, 247, 0.8)",
                  borderColor: "#a855f7",
                  borderWidth: 1,
                },
              ],
            }}
          />
        </div>
      </div>

      {/* Lien retour */}
      <div className="mt-6 text-center">
        <Link 
          href="/" 
          className="inline-flex items-center gap-2 text-zinc-400 hover:text-zinc-100 transition-colors"
        >
          ← Retour au tableau de bord
        </Link>
      </div>
    </main>
  );
}
