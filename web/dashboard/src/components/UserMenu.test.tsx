import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { UserMenu } from "@/components/UserMenu";
import * as api from "@/lib/api";

describe("UserMenu", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("shows sign in when no token", () => {
    render(
      <MemoryRouter>
        <UserMenu />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByTitle(/not signed in/i));
    expect(screen.getAllByText("Sign in").length).toBeGreaterThanOrEqual(1);
  });

  it("shows sign out when token exists", () => {
    api.setAdminToken("my-token");

    render(
      <MemoryRouter>
        <UserMenu />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByTitle(/admin user/i));
    expect(screen.getByText("Sign out")).toBeInTheDocument();
  });

  it("clears token and navigates to login on sign out", () => {
    api.setAdminToken("my-token");

    render(
      <MemoryRouter initialEntries={["/dashboard"]}>
        <Routes>
          <Route path="/dashboard" element={<UserMenu />} />
          <Route path="/login" element={<div>Login Page</div>} />
        </Routes>
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByTitle(/admin user/i));
    fireEvent.click(screen.getByText("Sign out"));

    expect(api.getAdminToken()).toBeNull();
    expect(screen.getByText("Login Page")).toBeInTheDocument();
  });
});
