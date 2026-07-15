import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

// cn is the canonical shadcn class-merging helper:
//   1. clsx resolves conditional class strings.
//   2. tailwind-merge dedupes overlapping Tailwind utilities
//      (e.g. `px-2 px-4` collapses to `px-4`).
export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
