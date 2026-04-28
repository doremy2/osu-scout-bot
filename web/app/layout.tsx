import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "osu! scout Power Rankings",
  description: "Current osu! tournament player power rankings"
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
