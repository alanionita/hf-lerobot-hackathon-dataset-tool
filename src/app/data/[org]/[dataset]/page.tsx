import { redirect } from "next/navigation";

export default async function DatasetRootPage(
  props: {
    params: Promise<{ org: string; dataset: string }>;
  }
) {
  const params = await props.params;
  const episodeN = process.env.EPISODES
    ?.split(/\s+/)
    .map((x) => parseInt(x.trim(), 10))
    .filter((x) => !isNaN(x))[0] ?? 0;

  redirect(`/data/${params.org}/${params.dataset}/episode_${episodeN}`);
}
