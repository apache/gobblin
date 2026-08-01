module.exports = {
  title: 'Apache Gobblin',
  tagline: 'A distributed data integration framework that simplifies common aspects of big data integration such as data ingestion, replication, organization and lifecycle management for both streaming and batch data ecosystems.',
  url: 'https://your-docusaurus-test-site.com',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'gobblin', // Usually your repo name.
  themeConfig: {
    hideableSidebar: true,
    sidebarCollapsible: true,
    prism: {
      additionalLanguages: ['java', 'properties'],
    },
    navbar: {
      title: 'Apache Gobblin',
      logo: {
        alt: 'Apache Gobblin logo',
        src: 'img/gobblin-logo.png',
      },
      items: [
        {
          to: 'docs/',
          activeBasePath: 'docs',
          label: 'Docs',
          position: 'left'
        },
        {
          href: 'https://communityinviter.com/apps/apache-gobblin/apache-gobblin',
          label: 'Slack',
          position: 'right'
        },
        {
          href: 'https://github.com/apache/gobblin',
          label: 'GitHub',
          position: 'right',
        },
        {
          to: 'downloads/',
          activeBasePath: 'downloads',
          label: 'Downloads',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Community',
          items: [
            {
              label: 'Slack',
              href: 'https://discordapp.com/invite/docusaurus',
            },
            {
              label: 'Stack Overflow',
              href: 'https://stackoverflow.com/questions/tagged/gobblin',
            },
          ],
        },
        {
          title: 'Apache',
          items: [
            {
              label: 'Foundation',
              href: 'https://www.apache.org/',
            },
            {
              label: 'License',
              href: 'https://www.apache.org/licenses',
            },
            {
              label: 'Events',
              href: 'https://www.apache.org/events/current-event',
            },
            {
              label: 'Security',
              href: 'https://www.apache.org/security',
            },
            {
              label: 'Sponsorship',
              href: 'https://www.apache.org/foundation/sponsorship.html',
            },
            {
              label: 'Thanks',
              href: 'https://www.apache.org/foundation/thanks.html',
            }
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} <a href="https://www.apache.org/">The Apache Software Foundation</a><br\>Apache, Apache Gobblin, the Apache feather and the Gobblin logo are trademarks of The Apache Software Foundation
`,
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl:
            'https://github.com/apache/gobblin-docs/edit/master/website/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
};
