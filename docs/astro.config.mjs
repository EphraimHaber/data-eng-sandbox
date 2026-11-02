// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

// GitHub Pages serves a project site at https://<owner>.github.io/<repo>/,
// so every URL Astro generates has to start with the repository name.
// If you move the site to a custom domain, set SITE to that domain, drop BASE,
// and add public/CNAME (see docs/README.md).
const SITE = 'https://ephraimhaber.github.io';
const BASE = '/data-eng-sandbox';
const REPO = 'https://github.com/EphraimHaber/data-eng-sandbox';

// https://astro.build/config
export default defineConfig({
	site: SITE,
	base: BASE,
	// One canonical URL per page. GitHub Pages serves folders with a trailing
	// slash, so we match that instead of relying on redirects.
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'Data-eng sandbox',
			description:
				'A local Postgres-to-Iceberg pipeline orchestrated by Airflow and run on Spark, for learning how the pieces fit.',
			logo: {
				src: './src/assets/core-mark.svg',
				alt: '',
			},
			favicon: '/favicon.svg',
			social: [{ icon: 'github', label: 'Source on GitHub', href: REPO }],
			editLink: {
				baseUrl: `${REPO}/edit/main/docs/`,
			},
			// Reads each page's last commit date. The deploy workflow checks out the
			// full history (fetch-depth: 0) so the date is real, not the CI clone time.
			lastUpdated: true,
			customCss: [
				'@fontsource-variable/archivo/wdth.css',
				'@fontsource-variable/atkinson-hyperlegible-next/wght.css',
				'@fontsource/ibm-plex-mono/400.css',
				'@fontsource/ibm-plex-mono/600.css',
				'./src/styles/theme.css',
			],
			components: {
				// Replaces Starlight's centred hero with the ice-core landing.
				Hero: './src/components/Hero.astro',
			},
			expressiveCode: {
				themes: ['nord', 'min-light'],
				styleOverrides: {
					borderRadius: '0.25rem',
					codeFontFamily: "'IBM Plex Mono', var(--sl-font-system-mono)",
					uiFontFamily: 'var(--__sl-font)',
				},
			},
			// Fails the build on broken internal links or missing #anchors, so a
			// renamed page can't silently break the published site.
			plugins: [starlightLinksValidator()],
			sidebar: [
				{
					label: 'Start here',
					items: [
						{ label: 'Start the stack', slug: 'start/quickstart' },
						{ label: 'What runs where', slug: 'start/architecture' },
					],
				},
				{ label: 'Pipeline', items: [{ autogenerate: { directory: 'pipeline' } }] },
				{ label: 'Operations', items: [{ autogenerate: { directory: 'operations' } }] },
				{ label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
			],
		}),
	],
});
