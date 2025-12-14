# Dashboard de l'IDS Parisbrest

Le dashboard a été créé grâce au framework NextJS.

## Installation

Télécharger les dépendances:
```
cd IDS-DASHBOARD/

pnpm i
```

Remplir les variables d'environnement:
```
nano .env.local
```

Exemples de variables d'environnements:
```
MONGODB_URI=<Lien vers la base de données>
MONGODB_DB=<Nom de la base de données>
```

Lancer le dashboard en local:
```
pnpm run dev
```