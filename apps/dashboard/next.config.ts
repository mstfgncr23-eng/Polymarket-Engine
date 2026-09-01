import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  // Fly/Docker dashboard image needs standalone. Vercel traces Next itself;
  // `output: "standalone"` omits next-server.js.nft.json and fails onBuildComplete.
  ...(process.env.VERCEL ? {} : { output: "standalone" as const }),
};

export default nextConfig;
