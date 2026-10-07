import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: { default: "osu! scout", template: "%s · osu! scout" },
  description: "osu! tournament analytics and scouting"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
