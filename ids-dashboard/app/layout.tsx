import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "ParisBrest - Dashboard",
  description: "Dashboard de l'IDS ParisBrest",
  icons: {
    icon: "/logo.png",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="fr" className="dark">
      <body className="dark:bg-black">
        {children}
      </body>
    </html>
  );
}
