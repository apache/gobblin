import React from "react";
import clsx from "clsx";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./styles.module.css";

const executionModes = [
    {
        title: "Standalone",
        imageUrl: "img/threads.gif",
        description: (
            <>
                Runs as standalone application on a single box. Also supports
                embedded mode.
            </>
        ),
    },
    {
        title: "Mapreduce Mode",
        imageUrl: "img/hadoop.png",
        description: (
            <>
                Runs as an mapreduce application on multiple Hadoop versions.
                Also supports <a href="https://azkaban.github.io/">Azkaban</a>{" "}
                for launcing mapreduce jobs.
            </>
        ),
    },
    {
        title: "Cluster / Yarn",
        imageUrl: "img/yarn_architecture.gif",
        description: (
            <>
                Runs as a standalone cluster with primary and worker nodes. This
                mode supports high availability, and can run on bare metals as
                well.
            </>
        ),
    },
    {
        title: "Cloud",
        imageUrl: "img/mesos-cluster.png",
        description: (
            <>
                Runs as elastic cluster on public cloud. This mode supports high
                availability.
            </>
        ),
    },
];

function ExecutionMode({ imageUrl, title, description }) {
    const imgUrl = useBaseUrl(imageUrl);
    return (
        <div className={clsx("col col--3", styles.executionMode)}>
            {imgUrl && (
                <div className="text--center">
                    <img
                        className={styles.executionModeImage}
                        src={imgUrl}
                        alt={title}
                    />
                </div>
            )}
            <h3>{title}</h3>
            <p>{description}</p>
        </div>
    );
}

function Home() {
    const context = useDocusaurusContext();
    const { siteConfig = {} } = context;
    return (
        <Layout
            title={`Hello from ${siteConfig.title}`}
            description="Description will go into a meta tag in <head />"
        >
            <header className={clsx("hero hero--primary", styles.heroBanner)}>
                <div className="container">
                    <h1 className="hero__title">{siteConfig.title}</h1>
                    <p className="hero__subtitle">{siteConfig.tagline}</p>
                    <div className={styles.buttons}>
                        <Link
                            className={clsx(
                                "button button--outline button--secondary button--lg",
                                styles.getStarted
                            )}
                            to={useBaseUrl("docs/Getting-Started")}
                        >
                            Get Started
                        </Link>
                        <Link
                            className={clsx(
                                "button button--outline button--secondary button--lg",
                                styles.getStarted
                            )}
                            to={useBaseUrl("downloads/")}
                        >
                            Download
                        </Link>
                    </div>
                </div>
            </header>
            <main>
                <div className="container">
                    <h1 className="hero__title" align="center">
                        Execution Modes
                    </h1>
                    {executionModes && executionModes.length > 0 && (
                        <section className={styles.executionMode}>
                            <div className="container">
                                <div className="row">
                                    {executionModes.map((props, idx) => (
                                        <ExecutionMode key={idx} {...props} />
                                    ))}
                                </div>
                            </div>
                        </section>
                    )}
                </div>
            </main>
        </Layout>
    );
}

export default Home;
